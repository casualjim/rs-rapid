use crate::{Endpoint, NodeId};
error_chain! {
  errors {
    InvalidConstraints(k: u32, h: u32, l: u32) {
      display("Arguments do not satisfy K >= 3 ∧ K > H >= L for (K: {}, H: {}, L: {})", k, h, l)
    }
    InvalidPermutations(k: u32) {
      display("Ring permutations must be greater than 0 (K: {})", k)
    }
    UUIDAlreadySeen(node: Endpoint, node_id: NodeId) {
      display("Endpoint add attempt with identifier already seen: {{ host: {}, identifier: {} }}", node, node_id)
    }
    NodeAlreadyInRing(node: Endpoint) {
      display("Endpoint add attempt but was already seen: {{ host: {} }}", node)
    }
    NodeNotInRing(node: Endpoint) {
      display("Endpoint not found: {{ host: {} }}", node)
    }
  }
}
