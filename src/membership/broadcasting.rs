use crate::remoting::rapid_request::Content;
use crate::remoting::{AlertMessage, BatchedAlertMessage};
use crate::{errors, Broadcaster, Endpoint, RapidRequest, RapidResponse, Transport};
use futures::prelude::*;
use futures::sync::mpsc;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

struct UnicastToAll<T: Transport> {
  members: Arc<RwLock<Vec<Endpoint>>>,
  client: T,
}

impl<T> UnicastToAll<T>
where
  T: Transport,
{
  fn new(client: T) -> Self {
    UnicastToAll {
      members: Arc::new(RwLock::new(vec![])),
      client,
    }
  }
}

impl<T> Broadcaster for UnicastToAll<T>
where
  T: Transport,
{
  fn broadcast(
    &mut self,
    request: &RapidRequest,
  ) -> Vec<Box<dyn Future<Item = RapidResponse, Error = errors::Error> + Send>> {
    self
      .members
      .clone()
      .read()
      .unwrap()
      .iter()
      .map(|ep| self.client.send(ep, request, 1))
      .collect()
  }

  fn set_membership(&mut self, members: &[Endpoint]) {
    self
      .members
      .clone()
      .write()
      .map(|mut mem| {
        let mut mems = members.to_vec();
        mems.shuffle(&mut thread_rng());
        *mem = mems;
      })
      .unwrap_or_else(|e| {
        error!("failed to set membership for broadcaster: {:?}", e);
      });
  }
}

struct AlertBatcher {
  client: Arc<dyn Broadcaster>,
  addr: Endpoint,
  sender: Option<mpsc::UnboundedSender<AlertMessage>>,
}

impl AlertBatcher {
  pub fn new(client: Arc<dyn Broadcaster>, addr: Endpoint) -> Self {
    AlertBatcher {
      client,
      addr,
      sender: None,
    }
  }

  pub fn start(&mut self) {
    let (sender, receiver) = mpsc::unbounded::<AlertMessage>();
    thread::spawn(|| {
      tokio_batch::Chunks::new(receiver, 100, Duration::from_secs(1)).for_each(|messages| {
        let v = BatchedAlertMessage {
          sender: Some(self.addr.clone()),
          messages,
        };
        let responses = self.client.clone().broadcast(&RapidRequest {
          content: Some(Content::BatchedAlertMessage(v)),
        });
        let f = futures::future::join_all(responses.map(|v|v.map_err(|_| ())).map_err(|_| ()).map(|_| ());
        f
      })
    });

    self.sender = Some(sender);
  }

  pub fn schedule(&self, msg: AlertMessage) {
    self
      .sender
      .as_ref()
      .map(|tx| {
        tx.unbounded_send(msg.clone())
          .map(|_| debug!("enqueued alert message: {:?}", &msg))
          .map_err(|e| error!("failed to enqueue {:?}: {}", &msg, e))
      })
      .unwrap_or(Ok(()));
  }
}

#[cfg(test)]
mod tests {
  use crate::membership::broadcasting::UnicastToAll;
  use crate::membership::tests::localhost;
  use crate::{errors, Broadcaster, Endpoint, RapidRequest, RapidResponse, Transport};
  use futures::{future, Future};

  #[test]
  fn broadcaster_sanity() {
    let mut endpoints = vec![];
    for n in 1..=10 {
      endpoints.push(localhost(n));
    }

    let mut bc = UnicastToAll::new(CountingClient { count: 0 });
    bc.set_membership(&endpoints);
    assert_eq!(bc.broadcast(&RapidRequest { content: None }).len(), 10);
    assert_eq!(bc.client.count, 10);
  }

  struct CountingClient {
    count: usize,
  }
  impl Transport for CountingClient {
    fn send(
      &mut self,
      to: &Endpoint,
      request: &RapidRequest,
      max_tries: usize,
    ) -> Box<dyn Future<Item = RapidResponse, Error = errors::Error> + Send> {
      self.count += 1;
      Box::new(future::ok(RapidResponse { content: None }))
    }
  }
}