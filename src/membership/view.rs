use crate::errors::{ErrorKind, Result};
use crate::remoting::{Endpoint, JoinStatusCode, NodeId};
use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Bound::{Excluded, Unbounded};
use twox_hash::XxHash64;

#[derive(Default, Debug, Clone)]
struct EndpointRing {
  endpoints: BTreeMap<u64, Endpoint>,
  ring_number: u64,
}

impl EndpointRing {
  fn new(initial: Option<&[Endpoint]>, ring_number: u64) -> Self {
    let mut endpoints = BTreeMap::new();
    if let Some(initial) = initial {
      initial.iter().for_each(|ep| {
        let checksum = EndpointRing::make_hash(ep, ring_number);
        endpoints.insert(checksum, ep.clone());
      });
    }
    EndpointRing { endpoints, ring_number }
  }

  pub fn contains(&self, endpoint: &Endpoint) -> bool {
    let key = self.checksum(endpoint);
    self.endpoints.contains_key(&key)
  }

  pub fn add(&mut self, endpoint: &Endpoint) {
    self.endpoints.insert(self.checksum(endpoint), endpoint.clone());
  }

  pub fn del(&mut self, endpoint: &Endpoint) {
    self.endpoints.remove(&self.checksum(endpoint));
  }

  pub fn len(&self) -> usize {
    return self.endpoints.len();
  }

  pub fn is_empty(&self) -> bool {
    return self.endpoints.is_empty();
  }

  pub fn higher(&self, subject: &Endpoint) -> Endpoint {
    let key = self.checksum(subject);
    self
      .endpoints
      .range((Excluded(key), Unbounded))
      .next()
      .map_or_else(|| self.endpoints.values().next().unwrap(), |(_, v)| v)
      .clone()
  }

  pub fn lower(&self, subject: &Endpoint) -> Endpoint {
    let key = self.checksum(subject);
    self
      .endpoints
      .range(..key)
      .next_back()
      .map_or_else(|| self.endpoints.values().rev().next().unwrap(), |(_, v)| v)
      .clone()
  }

  fn checksum(&self, endpoint: &Endpoint) -> u64 {
    EndpointRing::make_hash(endpoint, self.ring_number)
  }

  fn make_hash(endpoint: &Endpoint, seed: u64) -> u64 {
    let mut xxh = XxHash64::with_seed(seed);
    endpoint.hash(&mut xxh);
    xxh.finish()
  }

  fn to_vec(&self) -> Vec<Endpoint> {
    self.endpoints.iter().map(|(_, v)| v.clone()).collect()
  }
}

/// The Configuration object contains a list of nodes in the membership view as well as a list of UUIDs.
/// An instance of this object created from one MembershipView object contains the necessary information
/// to bootstrap an identical `View` object.
///
#[derive(Default, Debug)]
pub struct Configuration {
  configuration_id: i64,
  node_ids: Vec<NodeId>,
  endpoints: Vec<Endpoint>,
}

impl Configuration {
  fn new(node_ids: Vec<NodeId>, endpoints: Vec<Endpoint>) -> Self {
    let configuration_id = Configuration::make_configuration_id(&node_ids, &endpoints);

    Configuration {
      node_ids,
      endpoints,
      configuration_id,
    }
  }

  fn make_configuration_id(node_ids: &[NodeId], endpoints: &[Endpoint]) -> i64 {
    let mut xxh = XxHash64::default();
    node_ids.hash(&mut xxh);
    endpoints.hash(&mut xxh);
    xxh.finish() as i64
  }
}

/// Hosts K permutations of the memberlist that represent the monitoring relationship between nodes;
/// every node (an _observer_) observers its successor (a _subject_) on each ring.
#[derive(Default, Debug)]
pub struct View {
  k: usize,
  rings: Vec<EndpointRing>,
  seen_ids: BTreeSet<NodeId>,
  current_configuration: Configuration,
  should_update_configuration: bool,
}

impl View {
  pub fn new(k: usize, node_ids: Option<&[NodeId]>, endpoints: Option<&[Endpoint]>) -> Result<Self> {
    if k == 0 {
      return Err(ErrorKind::InvalidPermutations(k).into());
    }

    let mut rings: Vec<EndpointRing> = Vec::with_capacity(k);
    for ring_number in 0..k {
      rings.push(EndpointRing::new(endpoints, ring_number as u64));
    }

    let mut seen_ids: BTreeSet<NodeId> = BTreeSet::new();
    if let Some(ids) = node_ids {
      seen_ids.extend(ids.to_vec());
    }

    let config = Configuration::new(seen_ids.iter().map(|v| v.clone()).collect(), rings[0].to_vec());
    Ok(View {
      k,
      seen_ids,
      rings,
      current_configuration: config,
      should_update_configuration: true,
    })
  }

  /// Queries if a host with a logical identifier {@code uuid} is safe to add to the network.
  ///
  /// # Arguments
  ///
  /// * `node` - the joining node
  /// * `uuid` - the joining node's identifier.
  pub fn is_safe_to_join(&self, node: Endpoint, uuid: NodeId) -> JoinStatusCode {
    if self.rings[0].contains(&node) {
      JoinStatusCode::HostnameAlreadyInRing
    } else if self.seen_ids.contains(&uuid) {
      JoinStatusCode::UuidAlreadyInRing
    } else {
      JoinStatusCode::SafeToJoin
    }
  }

  /// Query if an identifier has been used by a node already.
  ///
  /// # Arguments
  ///
  /// * `identifier` - the identifier to query for
  ///
  pub fn is_identifier_present(&self, identifier: &NodeId) -> bool {
    self.seen_ids.contains(identifier)
  }

  /// Query if a host is part of the current membership set
  ///
  /// # Arguments
  ///
  /// * `endpoint` - the address of the host
  ///
  pub fn is_host_present(&self, endpoint: &Endpoint) -> bool {
    self.rings[0].contains(endpoint)
  }

  /// Add a node to all K rings and records its unique identifier
  ///
  /// # Arguments
  ///
  /// * `node` - the node to be added
  /// * `node_id` - the logical identifier of the node to be added
  ///
  pub fn ring_add(&mut self, endpoint: Endpoint, id: NodeId) -> Result<()> {
    if self.is_identifier_present(&id) {
      return Err(ErrorKind::UUIDAlreadySeen(endpoint, id).into());
    }

    if self.is_host_present(&endpoint) {
      return Err(ErrorKind::NodeAlreadyInRing(endpoint).into());
    }

    for k in 0..self.k {
      let ring = &mut self.rings[k];
      ring.add(&endpoint);
    }

    self.seen_ids.insert(id);
    self.should_update_configuration = true;
    Ok(())
  }

  /// Delete a host from all K rings.
  ///
  /// # Arguments
  ///
  /// * `node` - the host to remove
  ///
  pub fn ring_del(&mut self, node: &Endpoint) -> Result<()> {
    if !self.is_host_present(node) {
      return Err(ErrorKind::NodeNotInRing(node.clone()).into());
    }

    for k in 0..self.k {
      let ring = &mut self.rings[k];
      ring.del(node);
    }
    self.should_update_configuration = true;
    Ok(())
  }

  /// Get the list of endpoints in the k'th ring.
  ///
  /// # Arguments
  ///
  /// * `k` - the index of the ring to query
  ///
  pub fn ring(&self, ring: usize) -> Vec<Endpoint> {
    self
      .rings
      .get(ring)
      .map_or(vec![], |ring| ring.endpoints.values().map(|v| v.clone()).collect())
  }

  /// Get the ring number of an `observer` for a given `subject`, such that for index k
  /// `subject` is a successor of `subject` on `ring(k)`
  ///
  /// # Arguments
  ///
  /// * `observer` - the observer node
  /// * `subject` - the subject node
  ///
  pub fn ring_numbers(&self, observer: &Endpoint, subject: &Endpoint) -> Vec<i64> {
    if self.rings[0].len() <= 1 {
      return vec![];
    }
    self
      .predecessors_of(observer)
      .iter()
      .enumerate()
      .filter(|(_, endpoint)| *endpoint == subject)
      .map(|(ring_number, _)| ring_number as i64)
      .collect()
  }

  /// Get the number of nodes currently in the membership.
  pub fn len(&self) -> usize {
    return self.rings[0].len();
  }

  /// Returns the set of observers for `node`
  ///
  /// # Arguments
  ///
  /// * `node` - the input node
  ///
  pub fn observers_of(&self, node: &Endpoint) -> Result<Vec<Endpoint>> {
    if !self.is_host_present(node) {
      return Err(ErrorKind::NodeNotInRing(node.clone()).into());
    }

    if self.rings[0].len() <= 1 {
      return Ok(vec![]);
    }

    Ok((0..self.k).fold(vec![], |mut results, k| {
      results.push(self.rings[k].higher(node));
      results
    }))
  }

  /// Returns the set of nodes observed by `node`.
  ///
  /// # Arguments
  ///
  /// * `node` - the input node
  ///
  pub fn subjects_of(&self, node: &Endpoint) -> Result<Vec<Endpoint>> {
    if !self.is_host_present(node) {
      return Err(ErrorKind::NodeNotInRing(node.clone()).into());
    }

    if self.rings[0].len() <= 1 {
      return Ok(vec![]);
    }

    Ok(self.predecessors_of(node))
  }

  /// Returns the set of the expected observers of `node`, even before it is
  /// added to the ring. This is used during the bootstrap protocol to identify
  /// the nodes responsible for gatekeeping a joining peer.
  ///
  /// # Arguments
  ///
  /// * `node` - the input node
  ///
  pub fn expected_observers_of(&self, node: &Endpoint) -> Vec<Endpoint> {
    if self.rings[0].is_empty() {
      return vec![];
    }
    self.predecessors_of(node)
  }

  fn predecessors_of(&self, node: &Endpoint) -> Vec<Endpoint> {
    (0..self.k).fold(vec![], |mut result, k| {
      result.push(self.rings[k].lower(node));
      result
    })
  }

  pub fn configuration(&mut self) -> &Configuration {
    if self.should_update_configuration {
      self.current_configuration = Configuration::new(
        self.seen_ids.iter().map(|v| v.clone()).collect(),
        self.rings[0].to_vec(),
      );
      self.should_update_configuration = false;
    }
    return &self.current_configuration;
  }

  pub fn configuration_id(&mut self) -> i64 {
    return self.configuration().configuration_id;
  }
}

#[cfg(test)]
mod tests {
  use crate::errors::Result;
  use crate::membership::tests::K;
  use crate::membership::tests::{init, localhost};
  use crate::membership::View;
  use crate::{Endpoint, NodeId};
  use spectral::prelude::*;
  use std::collections::HashSet;
  use std::iter::FromIterator;
  use uuid::Uuid;

  fn verify_subjects(view: &View, addr: &Endpoint, expected_set_size: usize) {
    let subjects = view.subjects_of(addr);
    verify_endpoints_set(&subjects, expected_set_size);
  }

  fn verify_observers(view: &View, addr: &Endpoint, expected_set_size: usize) {
    let observers = view.observers_of(addr);
    verify_endpoints_set(&observers, expected_set_size);
  }

  fn verify_endpoints_set(endpoints: &Result<Vec<Endpoint>>, expected_set_size: usize) {
    assert_that(endpoints).is_ok();
    let endpoints = endpoints.as_ref().unwrap();
    assert_that(endpoints).has_length(K);
    let endpoints: HashSet<&Endpoint> = HashSet::from_iter(endpoints);
    assert_that(&endpoints.len()).is_equal_to(expected_set_size);
  }

  fn verify_endpoints_list(endpoints: &Result<Vec<Endpoint>>) {
    assert_that(endpoints).is_ok();
    let endpoints = endpoints.as_ref().unwrap();
    assert_that(endpoints).has_length(K);
  }

  #[test]
  fn node_configurations_across_multiple_views() {
    init();

    let mut view1 = View::new(K, None, None).unwrap();
    let mut view2 = View::new(K, None, None).unwrap();
    let num_nodes = 1000;

    let mut list1 = Vec::with_capacity(num_nodes);
    let mut list2 = Vec::with_capacity(num_nodes);

    for i in 0..num_nodes {
      let n = localhost(i as u16);
      let nid = Uuid::new_v3(&Uuid::NAMESPACE_URL, n.to_string().as_bytes());
      assert_that(&view1.ring_add(n, NodeId::from_uuid(nid))).is_ok();
      list1.push(view1.configuration_id());
    }
    for i in 0..num_nodes {
      let n = localhost(i as u16);
      let nid = Uuid::new_v3(&Uuid::NAMESPACE_URL, n.to_string().as_bytes());
      assert_that(&view2.ring_add(n, NodeId::from_uuid(nid))).is_ok();
      list2.push(view2.configuration_id());
    }

    assert_that(&list1).has_length(num_nodes);
    assert_that(&list2).has_length(num_nodes);
    for i in 0..(num_nodes - 1) {
      assert_that(&list1[i]).is_equal_to(&list2[i]);
    }
    assert_that(&list1[num_nodes - 1]).is_equal_to(&list2[num_nodes - 1]);
  }

  #[test]
  fn node_configuration_change() {
    let mut view = View::new(K, None, None).unwrap();
    let num_nodes = 1000;
    let mut collected = HashSet::new();

    for i in 0..num_nodes {
      assert_that(&view.ring_add(localhost(i), NodeId::new())).is_ok();
      collected.insert(view.configuration_id());
    }
    assert_that(&collected.len()).is_equal_to(num_nodes as usize);
  }

  #[test]
  fn node_unique_id_with_deletions() {
    let mut view = View::new(K, None, None).unwrap();

    let n1 = localhost(1);
    let id1 = NodeId::new();
    assert_that(&view.ring_add(n1.clone(), id1.clone())).is_ok();

    let n2 = localhost(2);
    let id2 = NodeId::new();
    assert_that(&view.ring_add(n2.clone(), id2.clone())).is_ok();

    assert_that(&view.ring_del(&n2)).is_ok();
    assert_that(&view.ring(0)).has_length(1);

    assert_that(&view.ring_add(n2.clone(), id2.clone())).is_err();

    assert_that(&view.ring_add(n2, NodeId::new())).is_ok();
    assert_that(&view.ring(0)).has_length(2);
  }

  #[test]
  fn node_unique_id_no_deletions() {
    let mut view = View::new(K, None, None).unwrap();

    let n1 = localhost(1);
    let id1 = NodeId::new();
    assert_that(&view.ring_add(n1.clone(), id1.clone())).is_ok();

    // Same host, same ID
    assert_that(&view.ring_add(n1.clone(), id1.clone())).is_err();

    // Same host, different ID
    assert_that(&view.ring_add(n1.clone(), NodeId::new())).is_err();

    // different host, same ID
    assert_that(&view.ring_add(localhost(2), id1.clone())).is_err();

    // different host, different ID
    assert_that(&view.ring_add(localhost(2), NodeId::new())).is_ok();

    assert_that(&view.ring(0)).has_length(2);
  }

  #[test]
  fn monitoring_relationship_during_bootstrap_multiple_nodes() {
    let mut view = View::new(K, None, None).unwrap();
    let num_nodes = 20;
    let server_port_base = 1234;
    let joining_node = localhost(server_port_base - 1);

    let mut num_observers = 0;
    for i in 0..num_nodes {
      let n = localhost(server_port_base + i);
      assert_that(&view.ring_add(n, NodeId::new())).is_ok();

      let actual_num_observers = view.expected_observers_of(&joining_node).len();
      assert_that!(actual_num_observers).is_greater_than_or_equal_to(num_observers);
      num_observers = actual_num_observers;
    }

    assert_that!(num_observers).is_greater_than_or_equal_to(K - 3);
    assert_that!(num_observers).is_less_than_or_equal_to(K);
  }

  #[test]
  fn monitoring_relationship_during_bootstrap() {
    let mut view = View::new(K, None, None).unwrap();
    let server_port = 1234;
    let n = localhost(server_port);
    assert_that(&view.ring_add(n.clone(), NodeId::new())).is_ok();

    let joining_node = localhost(server_port + 1);
    let expected_observers = view.expected_observers_of(&joining_node);
    assert_that(&expected_observers).has_length(K);
    let observer_set: HashSet<&Endpoint> = HashSet::from_iter(&expected_observers);
    assert_that(&observer_set.len()).is_equal_to(1);
    assert_that(&expected_observers[0]).is_equal_to(n);
  }

  #[test]
  fn monitoring_relationship_multiple_nodes() {
    // Verify the monitoring relationships in a multi node setting.
    let mut view = View::new(K, None, None).unwrap();
    let num_nodes = 1000;
    let mut nodes = vec![];

    for port in 0..num_nodes {
      let node = localhost(port);
      nodes.push(node.clone());
      assert_that(&view.ring_add(node, NodeId::new())).is_ok();
    }

    for i in 0..(num_nodes as usize) {
      verify_endpoints_list(&view.subjects_of(&nodes[i]));
      verify_endpoints_list(&view.observers_of(&nodes[i]));
    }
  }

  #[test]
  fn monitoring_relationship_three_nodes_with_delete() {
    // Verify the monitoring relationships in a three node setting
    let mut view = View::new(K, None, None).unwrap();

    let n1 = localhost(1);
    let n2 = localhost(2);
    let n3 = localhost(3);

    assert_that(&view.ring_add(n1.clone(), NodeId::new())).is_ok();
    assert_that(&view.ring_add(n2.clone(), NodeId::new())).is_ok();
    assert_that(&view.ring_add(n3, NodeId::new())).is_ok();

    verify_subjects(&view, &n1, 2);
    verify_observers(&view, &n1, 2);

    assert_that(&view.ring_del(&n2)).is_ok();
    verify_subjects(&view, &n1, 1);
    verify_observers(&view, &n1, 1);
  }

  #[test]
  fn monitoring_relationship_two_nodes() {
    // Verify the monitoring relationships in a two node setting
    let mut view = View::new(K, None, None).unwrap();

    let n1 = localhost(1);
    let n2 = localhost(2);

    assert_that(&view.ring_add(n1.clone(), NodeId::new())).is_ok();
    assert_that(&view.ring_add(n2, NodeId::new())).is_ok();

    verify_subjects(&view, &n1, 1);
    verify_observers(&view, &n1, 1);
  }

  // Verify the edge case of monitoring relationships in an empty view case.
  #[test]
  fn monitoring_relationship_empty() {
    let mut view = View::new(K, None, None).unwrap();

    let n = localhost(1);
    assert_that(&view.subjects_of(&n)).is_err();
    assert_that(&view.observers_of(&n)).is_err();
  }

  // Verify the edge case of monitoring relationships in a single node case.
  #[test]
  fn monitoring_relationship_edge() {
    let mut view = View::new(K, None, None).unwrap();

    let n1 = localhost(1);
    assert_that(&view.ring_add(n1.clone(), NodeId::new())).is_ok();

    let subjects = view.subjects_of(&n1);
    assert_that!(subjects).is_ok();
    assert_that!(subjects.unwrap()).is_empty();

    let observers = view.observers_of(&n1);
    assert_that!(observers).is_ok();
    assert_that!(observers.unwrap()).is_empty();

    let n2 = localhost(2);
    assert_that(&view.subjects_of(&n2)).is_err();
    assert_that(&view.observers_of(&n2)).is_err();
  }

  #[test]
  fn ring_additions_and_deletions() {
    let mut view = View::new(K, None, None).unwrap();
    let num_nodes = 10;

    for port in 0..num_nodes {
      assert_that!(view.ring_add(localhost(port), NodeId::new(),)).is_ok();
    }

    for port in 0..num_nodes {
      assert_that!(view.ring_del(&localhost(port))).is_ok();
    }

    for k in 0..K {
      let list = view.ring(k);
      assert_that!(list).is_empty();
    }
  }

  #[test]
  fn ring_deletions_only() {
    let mut view = View::new(K, None, None).unwrap();
    let num_nodes = 10;

    (0..num_nodes).for_each(|port| {
      let del = view.ring_del(&localhost(port));
      assert_that!(del).is_err();
    });
  }

  #[test]
  fn ring_readditions() {
    let mut view = View::new(K, None, None).unwrap();
    let num_nodes = 10;

    (0..num_nodes).for_each(|port| {
      let add_result = view.ring_add(localhost(port), NodeId::new());
      assert_that(&add_result).is_ok();
    });

    (0..K).for_each(|k| {
      let endpoints = view.ring(k);
      assert_that!(endpoints).has_length(num_nodes as usize);
    });

    (0..num_nodes).for_each(|port| {
      let add_result = view.ring_add(localhost(port), NodeId::new());
      assert_that(&add_result).is_err();
    });
  }

  #[test]
  fn multiple_ring_additions() {
    let mut view = View::new(K, None, None).unwrap();
    let num_nodes = 10;

    (0..num_nodes).for_each(|i| {
      let add_result = view.ring_add(localhost(i as u16), NodeId::new());
      assert_that(&add_result).is_ok();
    });
    (0..K).for_each(|k| {
      let endpoints = view.ring(k);
      assert_that!(endpoints).has_length(num_nodes);
    });
  }

  #[test]
  fn one_ring_addition() {
    let mut view = View::new(K, None, None).unwrap();
    let addr = localhost(123);
    let node_id = NodeId::new();
    let add_result = view.ring_add(addr.clone(), node_id);
    assert_that(&add_result).is_ok();

    for k in 0..K {
      let list = view.ring(k);
      assert_that(&list).has_length(1);
      assert_that(&list[0]).is_equal_to(&addr);
    }
  }

  #[test]
  fn fails_when_k_is_0() {
    let view_result = View::new(0, None, None);
    assert_that!(view_result).is_err();
  }
}
