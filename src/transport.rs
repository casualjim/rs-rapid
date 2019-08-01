use crate::membership::Service;
use crate::remoting::rapid_response::Content;
use crate::remoting::NodeStatus::Bootstrapping;
use crate::remoting::{MembershipService, MembershipServiceClient, ProbeResponse, RapidRequest, RapidResponse};
use crate::{errors, Endpoint};
use futures::{future, Async, Future};
use grpcio::{Channel, ChannelBuilder, ClientUnaryReceiver, Environment, RpcContext, UnarySink};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Clone)]
struct Config {}

struct Transport {
  env: Arc<Environment>,
  clients: Arc<Mutex<HashMap<Endpoint, Channel>>>,
  server: grpcio::Server,
}

impl crate::Transport for Transport {
  fn send(&mut self, to: &Endpoint, request: &RapidRequest, max_tries: usize) -> Box<crate::TransportFuture> {
    let mut clients = self.clients.lock().unwrap();

    let ch = clients.entry(to.clone()).or_insert_with(|| {
      ChannelBuilder::new(self.env.clone())
        .reuse_port(true)
        .connect(&to.to_string())
    });

    let fut = ClientResponseFuture {
      client: MembershipServiceClient::new(ch.clone()),
      request: request.clone(),
      tries: max_tries.max(1),
      current: None,
    };
    Box::new(fut)
  }
}

struct ClientResponseFuture {
  client: MembershipServiceClient,
  request: RapidRequest,
  tries: usize,
  current: Option<Result<ClientUnaryReceiver<RapidResponse>, grpcio::Error>>,
}

impl Future for ClientResponseFuture {
  type Item = RapidResponse;
  type Error = errors::Error;

  fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
    if let Some(result) = self.current.take() {
      match result {
        Ok(mut fut) => match fut.poll() {
          Ok(Async::Ready(resp)) => return Ok(Async::Ready(resp)),
          Ok(Async::NotReady) => return Ok(Async::NotReady),
          Err(e) => {
            debug!("request failed {} tries left", self.tries);
            if self.tries == 0 {
              return Err(errors::ErrorKind::Msg(format!("{:?}", e)).into());
            }
          }
        },
        Err(e) => {
          debug!("request failed {} tries left", self.tries);
          if self.tries == 0 {
            return Err(errors::ErrorKind::Msg(format!("{:?}", e)).into());
          }
        }
      }
    }
    self.current = Some(self.client.send_request_async(&self.request));
    self.tries -= 1;
    Ok(Async::NotReady)
  }
}

#[derive(Clone)]
pub struct Server {
  config: Config,
  svc: Arc<RwLock<Option<Service>>>,
}

impl Server {
  pub fn set_service(&mut self, svc: Service) {
    let guard = self.svc.clone();
    let mut val = guard.write().unwrap();
    *val = Some(svc);
    //    self.svc.store(Som?e(svc))
  }
}

impl Default for Server {
  fn default() -> Self {
    Server {
      config: Config {},
      svc: Arc::new(RwLock::new(None)),
    }
  }
}

impl MembershipService for Server {
  fn send_request(&mut self, ctx: RpcContext, req: RapidRequest, sink: UnarySink<RapidResponse>) {
    let _guard = self.svc.clone();
    let svc = _guard.read().unwrap();
    if svc.is_none() {
      let res = RapidResponse {
        content: Some(Content::ProbeResponse(ProbeResponse {
          status: Bootstrapping as i32,
        })),
      };
      ctx.spawn(
        sink
          .success(res)
          .map_err(move |e| error!("failed to respond during boostrap {:?}: {:?}", req, e)),
      );
      return;
    }
    let svc = svc.unwrap();
    ctx.spawn(future::lazy(move || {
      let op = match svc.handle_request(&req).wait() {
        Ok(resp) => match resp {
          Ok(resp) => sink.success(resp),
          Err(e) => sink.fail(grpcio::RpcStatus::new(
            grpcio::RpcStatusCode::INTERNAL,
            Some(format!("{:?}", e)),
          )),
        },
        Err(e) => sink.fail(grpcio::RpcStatus::new(
          grpcio::RpcStatusCode::INTERNAL,
          Some(format!("{:?}", e)),
        )),
      };

      op.map(|_| ()).map_err(|e| error!("failed to write response {:?}", e))
    }))
  }
}
