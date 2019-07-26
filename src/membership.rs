use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet, BTreeSet, BTreeMap};

const K_MIN: u32 = 3;

use crate::{Endpoint, NodeId};
use crate::errors::*;
use crate::remoting::{AlertMessage, EdgeStatus, JoinStatusCode};
use twox_hash::{XxHash, XxHash64};
use std::hash::Hash;
use std::hash::Hasher;
use std::cmp::Ordering;
use std::ops::Deref;

//pub struct Service;

#[derive(Default, Debug, Clone)]
struct EndpointRing {
  endpoints: BTreeMap<u64, Endpoint>,
  seed: u64,
}

impl EndpointRing {
  fn new(initial: Option<&[Endpoint]>, seed: u64) -> Self {
    let mut endpoints = BTreeMap::new();
    if let Some(initial) = initial {
      initial.iter().for_each(|ep| {
        let checksum = EndpointRing::make_hash(ep, seed);
        endpoints.insert(checksum, ep.clone());
      });
    }
    EndpointRing {
      endpoints,
      seed,
    }
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

  fn checksum(&self, endpoint: &Endpoint) -> u64 {
    EndpointRing::make_hash(endpoint, self.seed)
  }

  fn make_hash(endpoint: &Endpoint, seed: u64) -> u64 {
    let mut xxh = XxHash64::with_seed(seed);
    endpoint.hash(&mut xxh);
    xxh.finish()
  }

}


/// Hosts K permutations of the memberlist that represent the monitoring relationship between nodes;
/// every node (an _observer_) observers its successor (a _subject_) on each ring.
#[derive(Default, Debug)]
struct View {
  k: u32,
  rings: Vec<EndpointRing>,
  seen_ids: BTreeSet<NodeId>,
  current_configuration_id: i64,
  should_update_configuration: bool,
}

impl View {
  pub fn new(k: u32, node_ids: Option<&[NodeId]>, endpoints: Option<&[Endpoint]>) -> Result<Self> {
    if k == 0 {
      return Err(ErrorKind::InvalidPermutations(k).into());
    }
    let mut rings: Vec<EndpointRing> = Vec::with_capacity(k as usize);
    for ring_number in 0..k {
      rings.push(EndpointRing::new(endpoints, ring_number as u64));
    }
    let mut seen_ids: BTreeSet<NodeId> = BTreeSet::new();
    if let Some(ids) = node_ids {
      seen_ids.extend(ids.to_vec());
    }
    Ok(View {
      k,
      seen_ids,
      rings,
      ..Default::default()
    })
  }


  /// Queries if a host with a logical identifier {@code uuid} is safe to add to the network.
  ///
  /// ## Arguments
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
  /// ## Arguments
  ///
  /// * `identifier` - the identifier to query for
  ///
  pub fn is_identifier_present(&self, identifier: &NodeId) -> bool {
    self.seen_ids.contains(identifier)
  }

  /// Query if a host is part of the current membership set
  ///
  /// ## Arguments
  ///
  /// * `endpoint` - the address of the host
  ///
  pub fn is_host_present(&self, endpoint: &Endpoint) -> bool {
    self.rings[0].contains(endpoint)
  }

  /// Add a node to all K rings and records its unique identifier
  ///
  /// ## Arguments
  ///
  /// * `node` - the node to be added
  /// * `node_id` - the logical identifier of the node to be added
  ///
  pub fn ring_add(&mut self, endpoint: Endpoint, id: NodeId) -> Result<()> {
    let ring = &mut self.rings[0];
    if self.is_identifier_present(&id) {
      return Err(ErrorKind::UUIDAlreadySeen(endpoint, id).into());
    }

    if self.is_host_present(&endpoint) {
      return Err(ErrorKind::NodeAlreadyInRing(endpoint).into())
    }

    for k in 0..(self.k as usize) {
      let ring = &mut self.rings[k];
      ring.add(&endpoint);
    }

    self.seen_ids.insert(id);
    self.should_update_configuration = true;
    Ok(())
  }

  /// Delete a host from all K rings.
  ///
  /// ## Arguments
  ///
  /// * `node` - the host to remove
  ///
  pub fn ring_del(&mut self, node: &Endpoint) -> Result<()> {
    if !self.is_host_present(node) {
      return Err(ErrorKind::NodeNotInRing(node.clone()).into())
    }

    for k in 0..(self.k as usize) {
      let ring = &mut self.rings[k];
      ring.del(node);
    }
    self.should_update_configuration = true;
    Ok(())
  }

//  pub fn get_ring(&self, ring: usize) -> Vec<Endpoint> {
//    self.rings.get(ring).map_or(vec![], |ring| {
//
//    });
//    vec![]
//  }
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
  k: u32,
  h: u32,
  l: u32,

  proposal_count: usize,
  updates_in_progress: usize,
  reports_per_host: HashMap<Endpoint, HashMap<i32, Endpoint>>,
  proposal: Vec<Endpoint>,
  pre_proposal: HashSet<Endpoint>,
  seen_link_down_events: bool,
}

impl MultiNodeCutDetector {
  pub fn new(k: u32, h: u32, l: u32) -> Result<Self> {
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
    self.seen_link_down_events = false;
  }

  pub fn aggregate_for_proposal(&mut self, msg: &AlertMessage) -> Vec<Endpoint> {
    if msg.edge_status == EdgeStatus::Down.into() {
      debug!("seen a link down event: {}", msg.edge_status);
      self.seen_link_down_events = true
    }

    msg.ring_number.iter().fold(vec![], |mut results, ring_number| {
      if let Some(dst) = msg.edge_dst.as_ref() {
        let edge_dst: Endpoint = dst.to_owned().into();
        let mut reports_for_host = self.reports_per_host
          .entry(edge_dst.clone())
          .or_insert_with(|| HashMap::new());

        if reports_for_host.contains_key(ring_number) {
          debug!("stopping aggregation, because already seen this announcement for ring {}", ring_number);
          return results;
        }

        msg.edge_src.as_ref().map(|edge_src| {
          let edge_src: Endpoint = edge_src.to_owned().into();
          reports_for_host.entry(*ring_number).or_insert(edge_src.clone());
        });


        let report_count = reports_for_host.len();
        results.extend(self.calculate_aggregate(report_count, edge_dst.clone()));
      }
      results
    })
  }

  fn calculate_aggregate(&mut self, report_count: usize, dst: Endpoint) -> Vec<Endpoint> {
    if self.l as usize == report_count {
      debug!("we're at low watermark [num_reports: {}]", report_count);
      self.updates_in_progress += 1;
      self.pre_proposal.insert(dst.clone());
    }

    if self.h as usize != report_count {
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
      result.len());
    result
  }
}

#[cfg(test)]
mod tests {
  use crate::membership::MultiNodeCutDetector;
  use crate::Endpoint;
  use crate::remoting::{AlertMessage, EdgeStatus};
  use spectral::assert_that;

  const K: u32 = 10u32;
  const H: u32 = 8u32;
  const L: u32 = 2u32;

  fn alert_message(src: Endpoint, dst: Endpoint, status: EdgeStatus, ring_number: i32) -> AlertMessage {
    AlertMessage {
      edge_src: Some(src.into()),
      edge_dst: Some(dst.into()),
      edge_status: status as i32,
      configuration_id: -1,
      ring_number: vec![ring_number],
      ..Default::default()
    }
  }

  mod view {
    use std::collections::BTreeSet;
    use crate::membership::View;
    use spectral::prelude::*;

    #[test]
    fn fails_when_k_is_0() {
      let view_result = View::new(0, None, None);
      assert_that!(view_result).is_err();
    }
  }

  mod multinode_detector {
    use crate::membership::MultiNodeCutDetector;
    use std::thread::spawn;
    use std::sync::{Arc, Mutex};
    use crate::Endpoint;
    use crate::remoting::{AlertMessage, EdgeStatus};
    use crate::membership::tests::*;
    use spectral::prelude::*;
    use spectral::result::ResultAssertions;


    fn fill_rings(detector: &mut MultiNodeCutDetector, n: u32, dst: Endpoint) {
      for i in 0..n {
        verify_proposal_aggregation(
          detector,
          i as u16 + 1,
          dst.clone(),
          i as i32,
          0,
          0,
        );
      }
    }

    fn verify_proposal_aggregation(detector: &mut MultiNodeCutDetector, src_port: u16, dst: Endpoint, ring_number: i32, expected_len: usize, expected_proposals: usize) {
      let alert = alert_message(
        Endpoint::new("127.0.0.1".to_string(), src_port),
        dst,
        EdgeStatus::Up,
        ring_number,
      );

      let ret = detector.aggregate_for_proposal(&alert);
      assert_that(&ret).has_length(expected_len);
      assert_that(&detector.known_proposals()).is_equal_to(expected_proposals);
    }

    #[test]
    fn batch() {
      let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
      let src = Endpoint::new("127.0.0.1".to_string(), 1);
      let num_nodes = 3;
      let mut endpoints = vec![];
      for i in 0..num_nodes {
        endpoints.push(Endpoint::new("127.0.0.2".to_string(), 2 + i as u16));
      }

      let mut proposal = vec![];
      for endpoint in endpoints {
        for ring_number in 0..K {
          let alert = alert_message(src.clone(), endpoint.clone(), EdgeStatus::Up, ring_number as i32);
          proposal.extend(
            detector.aggregate_for_proposal(&alert),
          );
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

      fill_rings(&mut detector, (H - 1), dst1.clone());
      fill_rings(&mut detector, (L - 1), dst2.clone());
      fill_rings(&mut detector, (H - 1), dst3.clone());
      verify_proposal_aggregation(
        &mut detector,
        H as u16,
        dst1.clone(),
        (H - 1) as i32,
        0,
        0,
      );
      verify_proposal_aggregation(
        &mut detector,
        H as u16,
        dst3.clone(),
        (H - 1) as i32,
        2,
        1,
      );
    }

    #[test]
    fn multiple_blockers_past_h() {
      let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
      let dst1 = Endpoint::new("127.0.0.2".to_string(), 2);
      let dst2 = Endpoint::new("127.0.0.3".to_string(), 2);
      let dst3 = Endpoint::new("127.0.0.4".to_string(), 2);

      fill_rings(&mut detector, (H - 1), dst1.clone());
      fill_rings(&mut detector, (H - 1), dst2.clone());
      fill_rings(&mut detector, (H - 1), dst3.clone());

      // Unlike the 1 blocker and 3 blocker tests, this one adds more reports for
      // dst1 and dst3 past the H boundary.
      verify_proposal_aggregation(
        &mut detector,
        H as u16,
        dst1.clone(),
        (H - 1) as i32,
        0,
        0,
      );
      verify_proposal_aggregation(
        &mut detector,
        (H + 1) as u16,
        dst1.clone(),
        (H - 1) as i32,
        0,
        0,
      );
      verify_proposal_aggregation(
        &mut detector,
        H as u16,
        dst3.clone(),
        (H - 1) as i32,
        0,
        0,
      );
      verify_proposal_aggregation(
        &mut detector,
        (H + 1) as u16,
        dst3.clone(),
        (H - 1) as i32,
        0,
        0,
      );
      verify_proposal_aggregation(
        &mut detector,
        H as u16,
        dst2.clone(),
        (H - 1) as i32,
        3,
        1,
      );
    }

    #[test]
    fn blocking_three_blockers() {
      let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
      let dst1 = Endpoint::new("127.0.0.2".to_string(), 2);
      let dst2 = Endpoint::new("127.0.0.3".to_string(), 3);
      let dst3 = Endpoint::new("127.0.0.4".to_string(), 4);

      fill_rings(&mut detector, (H - 1), dst1.clone());
      fill_rings(&mut detector, (H - 1), dst2.clone());
      fill_rings(&mut detector, (H - 1), dst3.clone());

      verify_proposal_aggregation(
        &mut detector,
        H as u16,
        dst1.clone(),
        (H - 1) as i32,
        0,
        0,
      );

      verify_proposal_aggregation(
        &mut detector,
        H as u16,
        dst2.clone(),
        (H - 1) as i32,
        0,
        0,
      );

      verify_proposal_aggregation(
        &mut detector,
        H as u16,
        dst3.clone(),
        (H - 1) as i32,
        3,
        1,
      );
    }

    #[test]
    fn blocking_one_blocker() {
      let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
      let dst1 = Endpoint::new("127.0.0.2".to_string(), 2);
      let dst2 = Endpoint::new("127.0.0.3".to_string(), 3);

      fill_rings(&mut detector, (H - 1), dst1.clone());
      fill_rings(&mut detector, (H - 1), dst2.clone());

      verify_proposal_aggregation(
        &mut detector,
        H as u16,
        dst1.clone(),
        (H - 1) as i32,
        0,
        0,
      );

      verify_proposal_aggregation(
        &mut detector,
        H as u16,
        dst2.clone(),
        (H - 1) as i32,
        2,
        1,
      );
    }


    #[test]
    fn sanity_check() {
      let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
      let dst = Endpoint::new("127.0.0.2".to_string(), 2);

      fill_rings(&mut detector, (H - 1), dst.clone());
      verify_proposal_aggregation(
        &mut detector,
        H as u16,
        dst.clone(),
        (H - 1) as i32,
        1,
        1,
      );
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
