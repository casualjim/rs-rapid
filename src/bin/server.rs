#[macro_use]
extern crate log;
extern crate env_logger;

use futures::{Future, Stream};
use grpcio::{EnvBuilder, Environment, ServerBuilder};
use rs_rapid::remoting::create_membership_service;
use rs_rapid::transport;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn main() {
  //  future::result()
  std::env::set_var("RUST_LOG", "info,server=debug");
  env_logger::init();

  let running = Arc::new(AtomicBool::new(true));
  let r = running.clone();
  ctrlc::set_handler(move || {
    r.store(false, Ordering::SeqCst);
  })
  .expect("Error setting Ctrl-C handler");

  let env = Arc::new(EnvBuilder::new().name_prefix("rapid-grpc").build());
  let service = create_membership_service(transport::Server::default());
  let mut server = ServerBuilder::new(env)
    .register_service(service)
    .bind("localhost", 3579)
    .build()
    .expect("failed to build server");
  server.start();

  for &(ref host, port) in server.bind_addrs() {
    info!("listening on {}:{}", host, port);
  }
  while running.load(Ordering::SeqCst) {}
  server.shutdown().wait();
}
