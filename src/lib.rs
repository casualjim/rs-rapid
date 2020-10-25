// `error_chain!` can recurse deeply
#![recursion_limit = "1024"]
#![warn(clippy::all)]

#[macro_use]
extern crate log;

#[macro_use]
extern crate thiserror;

#[macro_use]
#[cfg(test)]
extern crate spectral;

use std::fmt::{Display, Formatter};
use std::mem;
use std::time::Duration;
use uuid::Uuid;
use std::convert::TryInto;

pub mod errors;
mod membership;
mod transport;

pub mod remoting {
  tonic::include_proto!("remoting");
}
// pub mod remoting;

pub use crate::remoting::{Endpoint, NodeId};
pub use crate::remoting::{RapidRequest, RapidResponse};

pub struct Cluster;

// pub trait Transport {
//   fn send(&mut self, to: &Endpoint, request: &RapidRequest, max_tries: usize) -> Box<TransportFuture>;
// }

pub struct Config {
  // pub transport: TransportConfig,
  pub failure_detector_interval: Duration,
}

impl Default for Config {
  fn default() -> Self {
    Config {
      // transport: TransportConfig::default(),
      failure_detector_interval: Duration::from_secs(1),
    }
  }
}

impl NodeId {
  pub fn from_uuid(id: Uuid) -> Self {
    let (hb, rest) = id.as_bytes().split_at(mem::size_of::<i64>());
    let (lb, _) = rest.split_at(mem::size_of::<i64>());
    NodeId {
      high: i64::from_be_bytes(hb.try_into().unwrap()),
      low: i64::from_be_bytes(lb.try_into().unwrap()),
    }
  }
  pub fn new() -> Self {
    return Self::from_uuid(Uuid::new_v4());
  }
}

impl Display for NodeId {
  fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
    let vals = [self.high.to_be_bytes(), self.low.to_be_bytes()].concat();
    let uid = Uuid::from_slice(&vals).map_err(|_| std::fmt::Error)?;
    write!(f, "{}", uid)
  }
}

impl AsRef<NodeId> for NodeId {
  fn as_ref(&self) -> &NodeId {
    self
  }
}

impl Endpoint {
  pub fn new<T: Into<Vec<u8>>>(hostname: T, port: u16) -> Self {
    Endpoint {
      hostname: hostname.into(),
      port: port as i32,
    }
  }
}

impl Display for Endpoint {
  fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
    write!(f, "{}:{}", std::str::from_utf8(&self.hostname).unwrap(), self.port)
  }
}

impl AsRef<Endpoint> for Endpoint {
  fn as_ref(&self) -> &Endpoint {
    self
  }
}

#[cfg(test)]
mod tests {
  use crate::remoting::{Endpoint, NodeId};
  use uuid::Uuid;

  #[test]
  fn random_node_id() {
    let node_id = NodeId::new();
    assert_ne!(node_id.high, 0);
    assert_ne!(node_id.low, 0);
    assert_ne!(node_id.high, node_id.low)
  }

  #[test]
  fn display_node_id() {
    let uuid = Uuid::new_v4();
    let value = format!("{}", NodeId::from_uuid(uuid.clone()));
    assert_eq!(uuid.to_string(), value);
    assert_eq!(uuid.to_string(), value.to_string());
  }

  #[test]
  fn display_endpoint() {
    let value = format!(
      "{}",
      Endpoint {
        hostname: "something".as_bytes().to_vec(),
        port: 2446,
      }
    );
    assert_eq!("something:2446", value);
    assert_eq!(
      "something:2345",
      Endpoint {
        hostname: "something".as_bytes().to_vec(),
        port: 2345,
      }
      .to_string()
    )
  }
}
