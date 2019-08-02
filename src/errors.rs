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

  foreign_links {
    Fmt(::std::fmt::Error);
    Io(::std::io::Error);
    Grpc(::grpcio::Error);
  }
}
