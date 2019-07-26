// `error_chain!` can recurse deeply
#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;

#[macro_use]
extern crate log;

use std::mem;
use std::fmt::{Display, Error, Formatter};
use uuid::Uuid;

pub mod errors;
mod membership;

//mod remoting {
//  include!(concat!(env!("OUT_DIR"), "/remoting.rs"));
//}
mod remoting;

use crate::remoting::{
  Endpoint as GRPCEndpoint,
  NodeId as GRPCNodeId,
};
use std::convert::TryInto;
use std::cmp::Ordering;
use twox_hash::XxHash64;

pub struct Cluster;

#[derive(Default, Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Hash)]
pub struct NodeId { // field order also determines comparator order, so Ord does the right thing
  pub high: i64,
  pub low: i64,
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
  fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
    let vals = [self.high.to_be_bytes(), self.low.to_be_bytes()].concat();
    let uid = Uuid::from_slice(&vals).map_err(|_| std::fmt::Error)?;
    write!(f, "{}", uid)
  }
}

impl From<GRPCNodeId> for NodeId {
  fn from(gnode_id: GRPCNodeId) -> Self {
    NodeId {
      high: gnode_id.high,
      low: gnode_id.low,
    }
  }
}

impl From<NodeId> for GRPCNodeId {
  fn from(node_id: NodeId) -> Self {
    GRPCNodeId {
      high: node_id.high,
      low: node_id.low,
      ..Default::default()
    }
  }
}

#[derive(Default, Debug, Ord, PartialOrd, Eq, PartialEq, Clone, Hash)]
pub struct Endpoint {
  pub hostname: String,
  pub port: u16,
}

impl Endpoint {
  pub fn new(hostname: String, port: u16) -> Self {
    Endpoint{
      hostname,
      port
    }
  }
}

impl Display for Endpoint {
  fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
    write!(f, "{}:{}", self.hostname, self.port)
  }
}

impl From<GRPCEndpoint> for Endpoint {
  fn from(ep: GRPCEndpoint) -> Self {
    Endpoint {
      hostname: ep.hostname,
      port: ep.port as u16,
    }
  }
}

impl From<Endpoint> for GRPCEndpoint {
  fn from(ep: Endpoint) -> Self {
    GRPCEndpoint {
      hostname: ep.hostname,
      port: ep.port as i32,
      ..Default::default()
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::remoting::{
    Endpoint as GRPCEndpoint,
    NodeId as GRPCNodeId,
  };
  use crate::{Endpoint, NodeId};
  use uuid::Uuid;

  #[test]
  fn random_node_id() {
    let node_id = NodeId::new();
    assert_ne!(node_id.high, 0);
    assert_ne!(node_id.low, 0);
    assert_ne!(node_id.high, node_id.low)
  }

  #[test]
  fn node_id_conversion() {
    let expected = NodeId::new();

    let actual: GRPCNodeId = expected.clone().into();
    assert_eq!(expected.high, actual.high);
    assert_eq!(expected.low, actual.low);

    let act2: NodeId = actual.clone().into();
    assert_eq!(expected.high, act2.high);
    assert_eq!(expected.low, act2.low);
  }

  #[test]
  fn display_node_id() {
    let uuid = Uuid::new_v4();
    let value = format!(
      "{}",
      NodeId::from_uuid(uuid.clone())
    );
    assert_eq!(uuid.to_string(), value);
    assert_eq!(uuid.to_string(), value.to_string());
  }

  #[test]
  fn endpoint_conversion() {
    let expected = Endpoint {
      hostname: "somewhere".to_string(),
      port: 3146,
    };

    let actual: GRPCEndpoint = expected.clone().into();
    assert_eq!(expected.hostname, actual.hostname);
    assert_eq!(expected.port as i32, actual.port);

    let exp2 = GRPCEndpoint {
      hostname: "someplace.com".to_string(),
      port: 443,
      ..Default::default()
    };
    let act2: Endpoint = exp2.clone().into();
    assert_eq!("someplace.com", act2.hostname);
    assert_eq!(443u16, act2.port);
  }

  #[test]
  fn display_endpoint() {
    let value = format!(
      "{}",
      Endpoint {
        hostname: "something".to_string(),
        port: 2446,
      }
    );
    assert_eq!("something:2446", value);
    assert_eq!(
      "something:2345",
      Endpoint {
        hostname: "something".to_string(),
        port: 2345,
      }
        .to_string()
    )
  }
}
