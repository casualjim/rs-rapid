use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

const K_MIN: usize = 3;

use crate::errors::*;
use crate::remoting::{AlertMessage, EdgeStatus, JoinStatusCode};
use crate::remoting::{Endpoint, NodeId};
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Bound::{Excluded, Unbounded};
use twox_hash::XxHash64;

//pub struct Service;

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
struct Configuration {
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
struct View {
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

///
/// A MultiNodeCutDetector is a filter that outputs a view change proposal about a node only if:
///
/// - there are H reports about a node.
/// - there is no other node about which there are more than L but less than H reports.
///
/// The output of this filter gives us almost-everywhere agreement
///
#[derive(Default, Debug)]
struct MultiNodeCutDetector {
  k: usize,
  h: usize,
  l: usize,

  proposal_count: usize,
  updates_in_progress: usize,
  reports_per_host: HashMap<Endpoint, HashMap<i32, Endpoint>>,
  proposal: Vec<Endpoint>,
  pre_proposal: HashSet<Endpoint>,
  seen_link_down_events: bool,
}

impl MultiNodeCutDetector {
  pub fn new(k: usize, h: usize, l: usize) -> Result<Self> {
    if h > k || l > h || k < K_MIN {
      return Err(ErrorKind::InvalidConstraints(k, h, l).into());
    }
    Ok(MultiNodeCutDetector {
      k,
      h,
      l,
      ..Default::default()
    })
  }

  /// Returns the known proposals in this cut detector
  pub fn known_proposals(&self) -> usize {
    self.proposal_count
  }

  /// Clears the state in this cut detector
  pub fn clear(&mut self) {
    self.reports_per_host.clear();
    self.pre_proposal.clear();
    self.proposal.clear();
    self.proposal_count = 0;
    self.updates_in_progress = 0;
    self.seen_link_down_events = false;
  }

  pub fn aggregate_for_proposal(&mut self, msg: &AlertMessage) -> Vec<Endpoint> {
    if msg.edge_status == EdgeStatus::Down.into() {
      debug!("seen a link down event: {}", msg.edge_status);
      self.seen_link_down_events = true
    }

    msg.ring_number.iter().fold(vec![], |mut results, ring_number| {
      results.extend(self.aggregate_proposal(
        msg.edge_src.as_ref(),
        msg.edge_dst.as_ref(),
        msg.edge_status,
        ring_number,
      ));
      results
    })
  }

  fn aggregate_proposal(
    &mut self,
    edge_src: Option<&Endpoint>,
    edge_dst: Option<&Endpoint>,
    status: i32,
    ring_number: &i32,
  ) -> Vec<Endpoint> {
    if status == EdgeStatus::Down as i32 {
      debug!("seen a link down event: {:?} > {:?}", edge_src, edge_dst);
      self.seen_link_down_events = true
    }
    //    self.reports_per_host;
    edge_dst.map_or(vec![], |dst| {
      let edge_dst: Endpoint = dst.clone();
      let reports_for_host = self
        .reports_per_host
        .entry(edge_dst.clone())
        .or_insert_with(|| HashMap::new());

      if reports_for_host.contains_key(ring_number) {
        debug!(
          "stopping aggregation, because already seen this announcement for ring {}",
          ring_number
        );
        return vec![];
      }

      edge_src.map(|edge_src| {
        let edge_src: Endpoint = edge_src.clone();
        reports_for_host.entry(*ring_number).or_insert(edge_src);
      });

      let report_count = reports_for_host.len();
      self.calculate_aggregate(report_count, edge_dst)
    })
  }

  fn calculate_aggregate(&mut self, report_count: usize, dst: Endpoint) -> Vec<Endpoint> {
    if self.l == report_count {
      debug!("we're at low watermark [num_reports: {}]", report_count);
      self.updates_in_progress += 1;
      self.pre_proposal.insert(dst.clone());
    }

    if self.h != report_count {
      return vec![];
    }

    self.pre_proposal.remove(&dst);
    self.proposal.push(dst);
    self.updates_in_progress -= 1;

    if self.updates_in_progress > 0 {
      return vec![];
    }

    self.proposal_count += 1;
    let result = std::mem::replace(&mut self.proposal, vec![]);
    debug!(
      "returning results because high watermark and there are no more updates in progress, results: {}",
      result.len()
    );
    result
  }

  /// Invalidates edges between nodes that are failing or have failed. This step may be skipped safely
  /// when there are no failing nodes. Returns a list of endpoints representing a view change proposal.
  ///
  /// # Arguments
  ///
  /// * `view` - MembershipView object required to find observer-subject relationships between failing nodes.
  ///
  pub fn invalidate_failing_edges(&mut self, view: &View) -> Vec<Endpoint> {
    if !self.seen_link_down_events {
      info!("no invalid links seen, returning");
      return vec![];
    }

    let mut proposals_to_return = vec![];
    for node_in_flux in self.pre_proposal.clone().iter() {
      let observers = if view.is_host_present(node_in_flux) {
        view.observers_of(node_in_flux).unwrap()
      } else {
        view.expected_observers_of(node_in_flux)
      };

      proposals_to_return.extend(observers.iter().enumerate().flat_map(|(ring_number, observer)| {
        if !self.proposal.contains(observer) && !self.pre_proposal.contains(observer) {
          return vec![];
        }
        let status = if view.is_host_present(node_in_flux) {
          EdgeStatus::Down
        } else {
          EdgeStatus::Up
        };
        self.aggregate_proposal(Some(observer), Some(node_in_flux), status as i32, &(ring_number as i32))
      }));
    }
    proposals_to_return
  }
}

#[cfg(test)]
mod tests {
  use crate::membership::MultiNodeCutDetector;
  use crate::remoting::{AlertMessage, EdgeStatus};
  use crate::Endpoint;
  use spectral::assert_that;

  const K: usize = 10usize;
  const H: usize = 8usize;
  const L: usize = 2usize;

  fn init() {
    std::env::set_var("RUST_LOG", "debug");
    let _ = env_logger::builder().is_test(true).try_init();
  }

  fn alert_message(src: Endpoint, dst: Endpoint, status: EdgeStatus, ring_number: i32) -> AlertMessage {
    AlertMessage {
      edge_src: Some(src),
      edge_dst: Some(dst),
      edge_status: status as i32,
      configuration_id: -1,
      ring_number: vec![ring_number],
      ..Default::default()
    }
  }

  fn localhost(port: u16) -> Endpoint {
    Endpoint::new("127.0.0.1".to_string(), port)
  }

  mod view {
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

  mod multinode_detector {
    use crate::membership::tests::*;
    use crate::membership::{MultiNodeCutDetector, View};
    use crate::remoting::{EdgeStatus, NodeId};
    use crate::Endpoint;
    use spectral::prelude::*;
    use std::collections::HashSet;

    fn fill_rings(detector: &mut MultiNodeCutDetector, n: usize, dst: Endpoint) {
      for i in 0..n {
        verify_proposal_aggregation(detector, i as u16 + 1, dst.clone(), i as i32, 0, 0);
      }
    }

    fn verify_proposal_aggregation(
      detector: &mut MultiNodeCutDetector,
      src_port: u16,
      dst: Endpoint,
      ring_number: i32,
      expected_len: usize,
      expected_proposals: usize,
    ) {
      let alert = alert_message(localhost(src_port), dst, EdgeStatus::Up, ring_number);

      let ret = detector.aggregate_for_proposal(&alert);
      assert_that(&ret).has_length(expected_len);
      assert_that(&detector.known_proposals()).is_equal_to(expected_proposals);
    }

    #[test]
    fn edge_invalidation() {
      let mut view = View::new(K, None, None).unwrap();
      let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
      let num_nodes = 30usize;

      let endpoints: Vec<Endpoint> = (0..num_nodes)
        .map(|port| {
          let n = localhost(port as u16);
          assert_that(&view.ring_add(n.clone(), NodeId::new())).is_ok();
          n
        })
        .collect();

      let dst: &Endpoint = &endpoints[0];
      let observers = view.observers_of(dst);
      assert_that(&observers).is_ok();
      let observers = observers.unwrap();
      assert_that(&observers).has_length(K);

      // This adds alerts from the observers[0, H - 1) of node dst.
      for i in 0..(H - 1) {
        let ret = detector.aggregate_for_proposal(&alert_message(
          (&observers[i]).clone(),
          dst.clone(),
          EdgeStatus::Down,
          i as i32,
        ));
        assert_that(&ret).is_empty();
        assert_that(&detector.proposal_count).is_equal_to(0);
      }

      // Next, we add alerts *about* observers[H, K) of node dst.
      let mut failed_observers = HashSet::with_capacity(K - H - 1);
      for i in (H - 1)..K {
        let watcher: &Endpoint = &observers[i];
        let watcher_watcher = view.observers_of(watcher);
        assert_that(&watcher_watcher).is_ok();
        let watcher_watcher = watcher_watcher.unwrap();
        failed_observers.insert(watcher.clone());

        for j in 0..K {
          let ret = detector.aggregate_for_proposal(&alert_message(
            (&watcher_watcher[j]).clone(),
            (&observers[i]).clone(),
            EdgeStatus::Down,
            j as i32,
          ));

          assert_that(&ret).is_empty();
          assert_that(&detector.proposal_count).is_equal_to(0);
        }
      }

      // At this point, (K - H - 1) observers of dst will be past H, and dst will be in H - 1. Link invalidation
      // should bring the failed observers and dst to the stable region.
      let ret = detector.invalidate_failing_edges(&view);
      assert_that(&ret).has_length(4);
      assert_that(&detector.proposal_count).is_equal_to(1);
      for node in ret.iter() {
        if !failed_observers.contains(node) {
          assert_that(node).is_equal_to(dst);
          continue;
        }
        assert_that(&failed_observers).contains(node);
      }
    }

    #[test]
    fn batch() {
      let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
      let src = localhost(1);
      let num_nodes = 3;
      let mut endpoints = vec![];
      for i in 0..num_nodes {
        endpoints.push(Endpoint::new("127.0.0.2".to_string(), 2 + i as u16));
      }

      let mut proposal = vec![];
      for endpoint in endpoints {
        for ring_number in 0..K {
          let alert = alert_message(src.clone(), endpoint.clone(), EdgeStatus::Up, ring_number as i32);
          proposal.extend(detector.aggregate_for_proposal(&alert));
        }
      }
      assert_that(&proposal).has_length(num_nodes);
    }

    #[test]
    fn below_l() {
      let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
      let dst1 = Endpoint::new("127.0.0.2".to_string(), 2);
      let dst2 = Endpoint::new("127.0.0.3".to_string(), 2);
      let dst3 = Endpoint::new("127.0.0.4".to_string(), 2);

      fill_rings(&mut detector, H - 1, dst1.clone());
      fill_rings(&mut detector, L - 1, dst2.clone());
      fill_rings(&mut detector, H - 1, dst3.clone());
      verify_proposal_aggregation(&mut detector, H as u16, dst1.clone(), (H - 1) as i32, 0, 0);
      verify_proposal_aggregation(&mut detector, H as u16, dst3.clone(), (H - 1) as i32, 2, 1);
    }

    #[test]
    fn multiple_blockers_past_h() {
      let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
      let dst1 = Endpoint::new("127.0.0.2".to_string(), 2);
      let dst2 = Endpoint::new("127.0.0.3".to_string(), 2);
      let dst3 = Endpoint::new("127.0.0.4".to_string(), 2);

      fill_rings(&mut detector, H - 1, dst1.clone());
      fill_rings(&mut detector, H - 1, dst2.clone());
      fill_rings(&mut detector, H - 1, dst3.clone());

      // Unlike the 1 blocker and 3 blocker tests, this one adds more reports for
      // dst1 and dst3 past the H boundary.
      verify_proposal_aggregation(&mut detector, H as u16, dst1.clone(), (H - 1) as i32, 0, 0);
      verify_proposal_aggregation(&mut detector, (H + 1) as u16, dst1.clone(), (H - 1) as i32, 0, 0);
      verify_proposal_aggregation(&mut detector, H as u16, dst3.clone(), (H - 1) as i32, 0, 0);
      verify_proposal_aggregation(&mut detector, (H + 1) as u16, dst3.clone(), (H - 1) as i32, 0, 0);
      verify_proposal_aggregation(&mut detector, H as u16, dst2.clone(), (H - 1) as i32, 3, 1);
    }

    #[test]
    fn blocking_three_blockers() {
      let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
      let dst1 = Endpoint::new("127.0.0.2".to_string(), 2);
      let dst2 = Endpoint::new("127.0.0.3".to_string(), 3);
      let dst3 = Endpoint::new("127.0.0.4".to_string(), 4);

      fill_rings(&mut detector, H - 1, dst1.clone());
      fill_rings(&mut detector, H - 1, dst2.clone());
      fill_rings(&mut detector, H - 1, dst3.clone());

      verify_proposal_aggregation(&mut detector, H as u16, dst1.clone(), (H - 1) as i32, 0, 0);

      verify_proposal_aggregation(&mut detector, H as u16, dst2.clone(), (H - 1) as i32, 0, 0);

      verify_proposal_aggregation(&mut detector, H as u16, dst3.clone(), (H - 1) as i32, 3, 1);
    }

    #[test]
    fn blocking_one_blocker() {
      let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
      let dst1 = Endpoint::new("127.0.0.2".to_string(), 2);
      let dst2 = Endpoint::new("127.0.0.3".to_string(), 3);

      fill_rings(&mut detector, H - 1, dst1.clone());
      fill_rings(&mut detector, H - 1, dst2.clone());

      verify_proposal_aggregation(&mut detector, H as u16, dst1.clone(), (H - 1) as i32, 0, 0);

      verify_proposal_aggregation(&mut detector, H as u16, dst2.clone(), (H - 1) as i32, 2, 1);
    }

    #[test]
    fn sanity_check() {
      let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
      let dst = Endpoint::new("127.0.0.2".to_string(), 2);

      fill_rings(&mut detector, H - 1, dst.clone());
      verify_proposal_aggregation(&mut detector, H as u16, dst.clone(), (H - 1) as i32, 1, 1);
    }
  }

  #[test]
  fn get_proposal_count() {
    let mut detector = MultiNodeCutDetector::new(10, 4, 2).unwrap();
    detector.proposal_count = 10;
    assert_that!(detector.known_proposals()).is_equal_to(10);
  }

  #[test]
  fn errors_invalid_constructor_args() {
    let smallk = MultiNodeCutDetector::new(2, 1, 1);
    assert!(smallk.is_err(), "expected an error result for small k");

    let lgth = MultiNodeCutDetector::new(3, 1, 2);
    assert!(lgth.is_err(), "expected an error result for l > h");

    let hgtk = MultiNodeCutDetector::new(3, 4, 1);
    assert!(hgtk.is_err(), "expected an error resutlf for h > k");
  }
}
