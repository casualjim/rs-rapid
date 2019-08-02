// `error_chain!` can recurse deeply
#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;

#[macro_use]
extern crate log;

use std::fmt::{Display, Formatter};
use std::mem;
use std::time::Duration;
use uuid::Uuid;

pub mod errors;
mod membership;
pub mod transport;

/*mod remoting {
  include!(concat!(env!("OUT_DIR"), "/remoting.rs"));

  const METHOD_MEMBERSHIP_SERVICE_SEND_REQUEST: ::grpcio::Method<RapidRequest, RapidResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/remoting.MembershipService/sendRequest",
    req_mar: ::grpcio::Marshaller {
      ser: ::grpcio::pr_ser,
      de: ::grpcio::pr_de,
    },
    resp_mar: ::grpcio::Marshaller {
      ser: ::grpcio::pr_ser,
      de: ::grpcio::pr_de,
    },
  };

  #[derive(Clone)]
  pub struct MembershipServiceClient {
    client: ::grpcio::Client,
  }

  impl MembershipServiceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
      MembershipServiceClient {
        client: ::grpcio::Client::new(channel),
      }
    }
    pub fn send_request_opt(&self, req: &RapidRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<RapidResponse> {
      self
        .client
        .unary_call(&METHOD_MEMBERSHIP_SERVICE_SEND_REQUEST, req, opt)
    }
    pub fn send_request(&self, req: &RapidRequest) -> ::grpcio::Result<RapidResponse> {
      self.send_request_opt(req, ::grpcio::CallOption::default())
    }
    pub fn send_request_async_opt(
      &self,
      req: &RapidRequest,
      opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<RapidResponse>> {
      self
        .client
        .unary_call_async(&METHOD_MEMBERSHIP_SERVICE_SEND_REQUEST, req, opt)
    }
    pub fn send_request_async(
      &self,
      req: &RapidRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<RapidResponse>> {
      self.send_request_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F)
    where
      F: ::futures::Future<Item = (), Error = ()> + Send + 'static,
    {
      self.client.spawn(f)
    }
  }

  pub trait MembershipService {
    fn send_request(&mut self, ctx: ::grpcio::RpcContext, req: RapidRequest, sink: ::grpcio::UnarySink<RapidResponse>);
  }

  pub fn create_membership_service<S: MembershipService + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s;
    builder = builder.add_unary_handler(&METHOD_MEMBERSHIP_SERVICE_SEND_REQUEST, move |ctx, req, resp| {
      instance.send_request(ctx, req, resp)
    });
    builder.build()
  }
}*/
mod remoting;

pub use crate::remoting::{Endpoint, NodeId};
pub use crate::remoting::{RapidRequest, RapidResponse};

use futures::Future;
use std::convert::TryInto;

type TransportFuture = dyn Future<Item = RapidResponse, Error = errors::Error> + Send;
pub struct Cluster;

pub trait Transport {
  fn send(&mut self, to: &Endpoint, request: &RapidRequest, max_tries: usize) -> Box<TransportFuture>;
}

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
  pub fn new<T: Into<String>>(hostname: T, port: u16) -> Self {
    Endpoint {
      hostname: hostname.into(),
      port: port as i32,
    }
  }
}

impl Display for Endpoint {
  fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
