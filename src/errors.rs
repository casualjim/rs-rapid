use crate::{Endpoint, NodeId};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RapidError {
  #[error("Arguments do not satisfy K >= 3 ∧ K > H >= L for (K: {0:?}, H: {1:?}, L: {2:?})")]
  InvalidConstraints(usize, usize, usize),
  #[error("Ring permutations must be greater than 0 (K: {0:?})")]
  InvalidPermutations(usize),
  #[error("Endpoint add attempt with identifier already seen: {{ host: {0:?}, identifier: {1:?} }}")]
  UUIDAlreadySeen(Endpoint, NodeId),
  #[error("Endpoint add attempt but was already seen: {{ host: {0:?} }}")]
  NodeAlreadyInRing(Endpoint),
  #[error("Endpoint not found: {{ host: {0:?} }}")]
  NodeNotInRing(Endpoint),
  #[error("Invalid node address: {0:?}")]
  InvalidAddr(String),
}
