// `error_chain!` can recurse deeply
#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;

#[macro_use]
extern crate log;

use std::fmt::{Display, Error, Formatter};
use std::mem;
use uuid::Uuid;

pub mod errors;
mod membership;

//mod remoting {
//  include!(concat!(env!("OUT_DIR"), "/remoting.rs"));
//}
mod remoting;

//use crate::remoting::{Endpoint, NodeId};
pub use self::remoting::{Endpoint, NodeId};
use std::convert::TryInto;
pub struct Cluster;

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

impl AsRef<NodeId> for NodeId {
  fn as_ref(&self) -> &NodeId {
    self
  }
}

impl Endpoint {
  pub fn new(hostname: String, port: u16) -> Self {
    Endpoint {
      hostname,
      port: port as i32,
    }
  }
}

impl Display for Endpoint {
  fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
    write!(f, "{}:{}", self.hostname, self.port)
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
