#[macro_use]
extern crate log;

use anyhow::*;
use futures::Future;
use rs_rapid::{
  remoting::{membership_service_client::MembershipServiceClient, rapid_request, PreJoinMessage},
  Endpoint, RapidRequest,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
  std::env::set_var("RUST_LOG", "info,client=debug,rs_rapid=debug");
  env_logger::init();

  let addr = "grpc://localhost:3579";
  let mut client = MembershipServiceClient::connect(addr).await?;
  let req = tonic::Request::new(RapidRequest {
    content: Some(rapid_request::Content::PreJoinMessage(PreJoinMessage {
      configuration_id: 1,
      ring_number: vec![1],
      node_id: None,
      sender: None,
    })),
  });
  client.send_request(req).await?;
  info!("running client");
  Ok(())
}
