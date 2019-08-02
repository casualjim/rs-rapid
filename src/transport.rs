use crate::membership::Service;
use crate::remoting::rapid_response::Content;
use crate::remoting::NodeStatus::Bootstrapping;
use crate::remoting::{
  create_membership_service, MembershipService, MembershipServiceClient, ProbeResponse, RapidRequest, RapidResponse,
};
use crate::{errors, Endpoint, TransportFuture};
use futures::{Async, Future};
use grpcio::{
  Channel, ChannelBuilder, ClientUnaryReceiver, EnvBuilder, Environment, RpcContext, ServerBuilder, UnarySink,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
  pub addr: Endpoint,
  pub timeout: Duration,
  pub join_timeout: Duration,
  pub probe_timeout: Duration,
  pub retries: usize,
}

impl Config {
  pub fn new(addr: Endpoint) -> Self {
    Config {
      addr,
      ..Default::default()
    }
  }
}

impl Default for Config {
  fn default() -> Self {
    Config {
      addr: Endpoint::new("", 0),
      timeout: Duration::from_secs(1),
      join_timeout: Duration::from_secs(5),
      probe_timeout: Duration::from_secs(1),
      retries: 5,
    }
  }
}

pub struct Transport {
  config: Arc<Config>,
  env: Arc<Environment>,
  clients: Arc<Mutex<HashMap<Endpoint, Channel>>>,
  server: Option<grpcio::Server>,
}

impl Transport {
  pub fn new(config: Arc<Config>) -> Self {
    Transport {
      config,
      env: Arc::new(EnvBuilder::new().name_prefix("rapid-grpc").build()),
      clients: Arc::new(Mutex::new(HashMap::new())),
      server: None,
    }
  }

  pub fn start(&mut self) -> errors::Result<()> {
    debug!("starting rapid listener");
    let cfg = self.config.clone();
    let svc = create_membership_service(Server::new(self.config.clone()));
    let mut srv = ServerBuilder::new(self.env.clone())
      .register_service(svc)
      .bind(cfg.addr.hostname.to_string(), cfg.addr.port as u16)
      .build()?;

    srv.start();
    for &(ref host, port) in srv.bind_addrs() {
      info!("listening on {}:{}", host, port);
    }
    self.server = Some(srv);

    Ok(())
  }

  pub fn stop(&mut self) {
    self
      .server
      .take()
      .map(|mut srv| srv.shutdown().wait().map_err(|e| error!("failed to shutdown: {:?}", e)));
  }
}

impl crate::Transport for Transport {
  fn send(&mut self, to: &Endpoint, request: &RapidRequest, max_tries: usize) -> Box<TransportFuture> {
    let mut clients = self.clients.lock().unwrap();
    let ch = clients.entry(to.clone()).or_insert_with(|| {
      ChannelBuilder::new(self.env.clone())
        .reuse_port(true)
        .connect(&to.to_string())
    });
    let fut = ClientResponseFuture {
      client: MembershipServiceClient::new(ch.clone()),
      request: request.clone(),
      state: RetryState::MakeRequest(max_tries),
    };
    Box::new(fut)
  }
}

enum RetryState {
  MakeRequest(usize),
  RequestInProgress(ClientUnaryReceiver<RapidResponse>, usize),
}

struct ClientResponseFuture {
  client: MembershipServiceClient,
  request: RapidRequest,
  state: RetryState,
}

impl Future for ClientResponseFuture {
  type Item = RapidResponse;
  type Error = errors::Error;

  fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
    loop {
      let new_state = match self.state {
        RetryState::MakeRequest(tries) => {
          if tries == 0 {
            return Err(format!("retries exhausted for {:?}", self.request).into());
          }
          match self.client.send_request_async(&self.request) {
            Ok(fut) => RetryState::RequestInProgress(fut, tries),
            Err(e) => {
              if tries == 1 {
                return Err(e.into());
              }
              RetryState::MakeRequest(tries - 1)
            }
          }
        }
        RetryState::RequestInProgress(ref mut fut, tries) => match fut.poll() {
          Ok(v) => return Ok(v),
          Err(e) => {
            if tries == 0 {
              return Err(e.into());
            }
            RetryState::MakeRequest(tries - 1)
          }
        },
      };
      self.state = new_state;
    }
  }
}

#[derive(Default, Clone)]
struct MembershipServiceHolder {
  svc: Arc<RwLock<Option<Arc<Service>>>>,
}

impl MembershipServiceHolder {
  pub fn set(&mut self, svc: Arc<Service>) {
    let mut val = self.svc.write().unwrap();
    *val = Some(svc)
  }

  pub fn get(&mut self) -> Option<Arc<Service>> {
    let guard = self.svc.clone().read().unwrap().as_ref().map(|v| v.clone());
    guard.clone()
  }
}

#[derive(Clone)]
pub struct Server {
  config: Arc<Config>,
  svc: MembershipServiceHolder,
}

impl Server {
  pub fn new(config: Arc<Config>) -> Self {
    Server {
      config,
      svc: MembershipServiceHolder::default(),
    }
  }
}

impl MembershipService for Server {
  fn send_request(&mut self, ctx: RpcContext, req: RapidRequest, sink: UnarySink<RapidResponse>) {
    debug!("handling {:?}", &req);
    let svc = self.svc.get();
    if svc.is_none() {
      let res = RapidResponse {
        content: Some(Content::ProbeResponse(ProbeResponse {
          status: Bootstrapping as i32,
        })),
      };
      ctx.spawn(
        sink
          .success(res)
          .map_err(move |e| error!("failed to respond during bootstrap {:?}: {:?}", req, e)),
      );
      return;
    }

    let svc = svc.unwrap();
    let fut = svc
      .handle_request(&req)
      .map_err(|e| grpcio::RpcStatus::new(grpcio::RpcStatusCode::INTERNAL, Some(format!("{:?}", e))))
      .then(move |fres| match fres {
        Ok(resp) => match resp {
          Ok(resp) => sink.success(resp),
          Err(e) => sink.fail(grpcio::RpcStatus::new(
            grpcio::RpcStatusCode::INTERNAL,
            Some(format!("{:?}", e)),
          )),
        },
        Err(e) => sink.fail(e),
      });
    ctx.spawn(fut.map(|_| ()).map_err(|e| error!("failed to write response {:?}", e)));
  }
}

#[cfg(test)]
mod tests {
  use crate::remoting::{create_membership_service, MembershipService, MembershipServiceClient};
  use crate::transport::*;
  use crate::{Endpoint, RapidRequest, RapidResponse, Transport};
  use futures::prelude::*;
  use grpcio::{ChannelBuilder, EnvBuilder, RpcContext, RpcStatus, ServerBuilder, UnarySink};
  use spectral::prelude::*;
  use std::sync::Arc;

  pub fn init() {
    std::env::set_var("RUST_LOG", "debug");
    let _ = env_logger::builder().is_test(true).try_init();
  }

  #[test]
  fn serve_requests() {
    let config = Config::new(Endpoint::new("localhost", 1358));
    let mut trans = super::Transport::new(Arc::new(config));
    assert_that(&trans.start()).is_ok();

    let ch = ChannelBuilder::new(trans.env.clone())
      .reuse_port(true)
      .connect(&trans.config.addr.to_string());
    let client = MembershipServiceClient::new(ch);
    let res = client.send_request(&RapidRequest { content: None });

    assert_that(&res).is_ok();
    trans.stop();
  }

  #[test]
  fn client_retries_success() {
    let addr = Endpoint::new("localhost", 1359);
    let svc = create_membership_service(Fail4Times { count: 0 });
    let env = Arc::new(EnvBuilder::new().name_prefix("rapid-test").build());
    let srv = ServerBuilder::new(env.clone())
      .register_service(svc)
      .bind((&addr).hostname.to_string(), (&addr).port as u16)
      .build();
    assert_that(&srv).is_ok();
    let mut srv = srv.unwrap();

    srv.start();
    let mut trans = super::Transport::new(Arc::new(Config::new(addr.clone())));
    let fur = trans.send(&addr, &RapidRequest { content: None }, 5);
    assert_that(&fur.wait()).is_ok();
    assert_that(&srv.shutdown().wait()).is_ok();
  }

  #[test]
  fn client_retries_fail() {
    let addr = Endpoint::new("localhost", 1357);
    let svc = create_membership_service(Fail4Times { count: 0 });
    let env = Arc::new(EnvBuilder::new().name_prefix("rapid-test").build());
    let srv = ServerBuilder::new(env.clone())
      .register_service(svc)
      .bind((&addr).hostname.to_string(), (&addr).port as u16)
      .build();
    assert_that(&srv).is_ok();
    let mut srv = srv.unwrap();

    srv.start();
    let mut trans = super::Transport::new(Arc::new(Config::new(addr.clone())));
    let fur = trans.send(&addr, &RapidRequest { content: None }, 3);
    assert_that(&fur.wait()).is_err();
    assert_that(&srv.shutdown().wait()).is_ok();
  }

  #[derive(Clone)]
  struct Fail4Times {
    count: usize,
  }
  impl MembershipService for Fail4Times {
    fn send_request(&mut self, ctx: RpcContext, req: RapidRequest, sink: UnarySink<RapidResponse>) {
      debug!("got request {}", &self.count);
      if self.count == 4 {
        sink.success(RapidResponse { content: None });
        return;
      }
      self.count += 1;
      sink.fail(RpcStatus::new(
        grpcio::RpcStatusCode::INTERNAL,
        Some("expected".to_string()),
      ));
    }
  }
}
