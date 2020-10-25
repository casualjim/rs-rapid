use crate::remoting::rapid_request::Content as Request;
use crate::remoting::rapid_response::Content as Response;
use anyhow::*;
use async_trait::async_trait;
use std::pin::Pin;
use tokio::stream::Stream;

fn broadcast() {
  let ch = tokio::sync::watch::channel("".to_string());
  ()
}

#[async_trait]
pub trait Client {
  async fn send(&mut self, request: Request) -> Result<Response>;
}

#[async_trait]
pub trait Server {}

#[async_trait]
pub trait Broadcaster {
  async fn broadcast(&mut self, req: Request) -> Result<()>;
}
