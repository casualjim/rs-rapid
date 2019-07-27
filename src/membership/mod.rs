mod cut_detector;
mod view;

use self::view::View;

const K_MIN: usize = 3;

//pub struct Service;

#[cfg(test)]
mod tests {
  use crate::remoting::{AlertMessage, EdgeStatus};
  use crate::Endpoint;
  use spectral::assert_that;

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
    Endpoint::new("127.0.0.1".to_string(), port)
  }

}
