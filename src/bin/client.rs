#[macro_use]
extern crate log;

use grpcio::{ChannelBuilder, EnvBuilder, Environment};
use rs_rapid::remoting::{MembershipServiceClient, RapidRequest};
use std::sync::Arc;

fn main() {
  std::env::set_var("RUST_LOG", "info,client=debug");
  env_logger::init();

  let env = Arc::new(EnvBuilder::new().build());
  let ch = ChannelBuilder::new(env).reuse_port(true).connect("localhost:3579");
  let client = MembershipServiceClient::new(ch);

  let res = client
    .send_request(&RapidRequest { content: None })
    .expect("failed to send the request");
  info!("got response: {:?}", res);
}
