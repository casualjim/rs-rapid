use super::View;
use super::K_MIN;
use crate::errors::*;
use crate::remoting::Endpoint;
use crate::remoting::{AlertMessage, EdgeStatus};
use std::collections::{HashMap, HashSet};

///
/// A MultiNodeCutDetector is a filter that outputs a view change proposal about a node only if:
///
/// - there are H reports about a node.
/// - there is no other node about which there are more than L but less than H reports.
///
/// The output of this filter gives us almost-everywhere agreement
///
#[derive(Default, Debug)]
pub struct MultiNodeCutDetector {
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
  use super::MultiNodeCutDetector;
  use crate::membership::tests::{alert_message, localhost};
  use crate::membership::tests::{H, K, L};
  use crate::membership::View;
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
      endpoints.push(Endpoint::new("127.0.0.2", 2 + i as u16));
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
    let dst1 = Endpoint::new("127.0.0.2", 2);
    let dst2 = Endpoint::new("127.0.0.3", 2);
    let dst3 = Endpoint::new("127.0.0.4", 2);

    fill_rings(&mut detector, H - 1, dst1.clone());
    fill_rings(&mut detector, L - 1, dst2.clone());
    fill_rings(&mut detector, H - 1, dst3.clone());
    verify_proposal_aggregation(&mut detector, H as u16, dst1.clone(), (H - 1) as i32, 0, 0);
    verify_proposal_aggregation(&mut detector, H as u16, dst3.clone(), (H - 1) as i32, 2, 1);
  }

  #[test]
  fn multiple_blockers_past_h() {
    let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
    let dst1 = Endpoint::new("127.0.0.2", 2);
    let dst2 = Endpoint::new("127.0.0.3", 2);
    let dst3 = Endpoint::new("127.0.0.4", 2);

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
    let dst1 = Endpoint::new("127.0.0.2", 2);
    let dst2 = Endpoint::new("127.0.0.3", 3);
    let dst3 = Endpoint::new("127.0.0.4", 4);

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
    let dst1 = Endpoint::new("127.0.0.2", 2);
    let dst2 = Endpoint::new("127.0.0.3", 3);

    fill_rings(&mut detector, H - 1, dst1.clone());
    fill_rings(&mut detector, H - 1, dst2.clone());

    verify_proposal_aggregation(&mut detector, H as u16, dst1.clone(), (H - 1) as i32, 0, 0);

    verify_proposal_aggregation(&mut detector, H as u16, dst2.clone(), (H - 1) as i32, 2, 1);
  }

  #[test]
  fn sanity_check() {
    let mut detector = MultiNodeCutDetector::new(K, H, L).unwrap();
    let dst = Endpoint::new("127.0.0.2", 2);

    fill_rings(&mut detector, H - 1, dst.clone());
    verify_proposal_aggregation(&mut detector, H as u16, dst.clone(), (H - 1) as i32, 1, 1);
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
    assert!(hgtk.is_err(), "expected an error result for h > k");
  }
}
