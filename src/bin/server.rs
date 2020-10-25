#[macro_use]
extern crate log;
extern crate env_logger;

use rs_rapid::remoting::membership_service_server::{MembershipService, MembershipServiceServer};
use rs_rapid::RapidResponse;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  std::env::set_var("RUST_LOG", "info,server=debug,rs_rapid=debug");
  env_logger::init();

  let addr = "[::]:3579".parse().unwrap();

  info!("MembershipService listening on: {}", addr);

  Server::builder()
    .add_service(MembershipServiceServer::new(RapidServer::default()))
    .serve(addr)
    .await?;

  Ok(())
}

#[derive(Default)]
struct RapidServer;

#[tonic::async_trait]
impl MembershipService for RapidServer {
  async fn send_request(
    &self,
    _request: tonic::Request<rs_rapid::RapidRequest>,
  ) -> Result<tonic::Response<rs_rapid::RapidResponse>, tonic::Status> {
    info!("got a request: {:?}", _request);
    Ok(tonic::Response::new(RapidResponse { content: None }))
  }
}
