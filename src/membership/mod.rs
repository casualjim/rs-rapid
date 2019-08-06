mod broadcasting;
mod cut_detector;
mod view;

use self::cut_detector::MultiNodeCutDetector;
use self::view::View;
use crate::errors;
use crate::remoting::rapid_request::Content;
use crate::remoting::{JoinResponse, PreJoinMessage, RapidRequest, RapidResponse};
use crate::TransportFuture;
use errors::Result;
use futures::{future, Future};
use std::sync::{Arc, Mutex};

const K_MIN: usize = 3;

#[derive(Clone)]
pub struct Service {
  cut_detection: Arc<Mutex<MultiNodeCutDetector>>,
  view: Arc<Mutex<View>>,
  update_lock: Arc<Mutex<()>>,
}

impl Service {
  pub fn handle_request(
    &self,
    request: &RapidRequest,
  ) -> impl Future<Item = Result<RapidResponse>, Error = errors::Error> {
    //    match request.content {
    //      Some(ref cnt) => match cnt {
    //        Content::PreJoinMessage(v) => self.handle_pre_join(v),
    //        _ => future::ok(Ok(RapidResponse { content: None })),
    //      },
    //      None => future::err("e".into()),
    //    }
    future::err("not implemented".into())
  }

  fn handle_pre_join(&self, msg: &PreJoinMessage) -> impl Future<Item = Result<RapidResponse>, Error = errors::Error> {
    use crate::remoting::rapid_response::Content;
    future::ok(Ok(RapidResponse { content: None }))
  }
}

#[cfg(test)]
mod tests {
  use crate::remoting::{AlertMessage, EdgeStatus};
  use crate::Endpoint;

  pub const K: usize = 10usize;
  pub const H: usize = 8usize;
  pub const L: usize = 2usize;

  pub fn init() {
    std::env::set_var("RUST_LOG", "debug");
    let _ = env_logger::builder().is_test(true).try_init();
  }

  pub fn alert_message(src: Endpoint, dst: Endpoint, status: EdgeStatus, ring_number: i32) -> AlertMessage {
    AlertMessage {
      edge_src: Some(src),
      edge_dst: Some(dst),
      edge_status: status as i32,
      configuration_id: -1,
      ring_number: vec![ring_number],
      ..Default::default()
    }
  }

  pub fn localhost(port: u16) -> Endpoint {
    Endpoint::new("127.0.0.1", port)
  }

}
