#[macro_use]
extern crate log;
extern crate env_logger;

use rs_rapid::{transport, Endpoint};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn main() {
  //  future::result()
  std::env::set_var("RUST_LOG", "info,server=debug,rs_rapid=debug");
  env_logger::init();

  let running = Arc::new(AtomicBool::new(true));
  let r = running.clone();
  ctrlc::set_handler(move || {
    r.store(false, Ordering::SeqCst);
  })
  .expect("Error setting Ctrl-C handler");

  let addr = Endpoint::new("localhost", 3579);
  let cfg = transport::Config::new(addr);
  let mut trans = transport::Transport::new(Arc::new(cfg));
  trans.start().expect("unable to start server");

  while running.load(Ordering::SeqCst) {}
  trans.stop();
}
