use crate::{Endpoint, NodeId};
error_chain! {
  errors {
    InvalidConstraints(k: usize, h: usize, l: usize) {
      display("Arguments do not satisfy K >= 3 ∧ K > H >= L for (K: {}, H: {}, L: {})", k, h, l)
    }
    InvalidPermutations(k: usize) {
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
    InvalidAddr(addr: String) {
      display("Invalid node address: {}", addr)
    }
  }
}
/*

#[derive(Debug, Error)]
pub enum RapidError {
  #[error(display = "{}", _0)]
  Msg(String),
  #[error(
    display = "Arguments do not satisfy K >= 3 ∧ K > H >= L for (K: {}, H: {}, L: {})",
    _0,
    _1,
    _2
  )]
  InvalidConstraints(usize, usize, usize),
  #[error(display = "Ring permutations must be greater than 0 (K: {})", _0)]
  InvalidPermutations(usize),
  #[error(
    display = "Endpoint add attempt with identifier already seen: {{ host: {}, identifier: {} }}",
    _0,
    _1
  )]
  UUIDAlreadySeen(Endpoint, NodeId),
  #[error(display = "Endpoint add attempt but was already seen: {{ host: {} }}", _0)]
  NodeAlreadyInRing(Endpoint),
  #[error(display = "Endpoint not found: {{ host: {} }}]", _0)]
  NodeNotInRing(Endpoint),
  #[error(display = "Invalid node address: {}", _0)]
  InvalidAddr(String),
}
*/
