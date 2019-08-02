#[macro_use]
extern crate log;

use futures::Future;
use rs_rapid::Transport;
use rs_rapid::{transport, Endpoint, RapidRequest};
use std::sync::Arc;

fn main() {
  std::env::set_var("RUST_LOG", "info,client=debug,rs_rapid=debug");
  env_logger::init();

  //  let env = Arc::new(EnvBuilder::new().build());
  //  let ch = ChannelBuilder::new(env).reuse_port(true).connect("localhost:3579");
  //  let client = MembershipServiceClient::new(ch);
  let addr = Endpoint::new("localhost", 3579);
  let cfg = transport::Config::new(addr.clone());
  let mut trans = transport::Transport::new(Arc::new(cfg));

  trans
    .send(&addr, &RapidRequest { content: None }, 1)
    .map(|res| info!("got response: {:?}", res))
    .map_err(|e| error!("failed to get response: {:?}", e))
    .wait()
    .expect("failed to get result");
}
