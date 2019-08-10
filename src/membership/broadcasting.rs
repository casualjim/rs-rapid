use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use futures::prelude::*;
use futures::sync::mpsc;
use rand::seq::SliceRandom;
use rand::thread_rng;

use crate::remoting::rapid_request::Content;
use crate::remoting::{AlertMessage, BatchedAlertMessage};
use crate::{errors, Broadcaster, Endpoint, RapidRequest, RapidResponse, Transport};

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
  client: Arc<Mutex<dyn Broadcaster + Send>>,
  addr: Endpoint,
  sender: Option<mpsc::Sender<AlertMessage>>,
}

impl AlertBatcher {
  pub fn new(client: Arc<Mutex<dyn Broadcaster + Send>>, addr: Endpoint) -> Self {
    AlertBatcher {
      client,
      addr,
      sender: None,
    }
  }

  pub fn start(&mut self) {
    let (sender, receiver) = mpsc::channel::<AlertMessage>(500);
    let addr = self.addr.clone();
    let client = self.client.clone();
    thread::spawn(move || {
      tokio_batch::Chunks::new(receiver, 100, Duration::from_secs(1))
        .map_err(|e| error!("failed to send alert batch: {:?}", e))
        .for_each(|messages| {
          let v = BatchedAlertMessage {
            sender: Some(addr.clone()),
            messages,
          };
          let responses = client.lock().unwrap().broadcast(&RapidRequest {
            content: Some(Content::BatchedAlertMessage(v)),
          });
          futures::future::join_all(responses).map(|_| ()).map_err(|_| ())
        })
        .wait()
        .map_err(|_| ())
        .unwrap_or_default();
      debug!("finished batching alerts")
    });

    self.sender = Some(sender);
  }

  pub fn stop(&mut self) {
    self.sender.take();
  }

  pub fn schedule(&mut self, msg: AlertMessage) {
    self
      .sender
      .as_mut()
      .map(|tx| {
        tx.try_send(msg.clone())
          .map(|_| debug!("enqueued alert message: {:?}", &msg))
          .map_err(|e| error!("failed to enqueue {:?}: {}", &msg, e))
      })
      .unwrap_or(Ok(()))
      .unwrap();
  }
}

#[cfg(test)]
mod tests {
  use futures::{future, Future};

  use crate::membership::broadcasting::{AlertBatcher, UnicastToAll};
  use crate::membership::tests::{init, localhost};
  use crate::remoting::AlertMessage;
  use crate::{errors, Broadcaster, Endpoint, RapidRequest, RapidResponse, Transport};
  use std::sync::{Arc, Mutex, RwLock};
  use std::thread;
  use std::time::Duration;

  #[test]
  fn alertbatcher() {
    init();
    let client = Arc::new(Mutex::new(UnicastToAll::new(CountingClient { count: 0 })));
    let addr = localhost(1);
    let mut alerter = AlertBatcher::new(client, addr);
    alerter.start();
    alerter.schedule(AlertMessage {
      edge_src: None,
      edge_dst: None,
      edge_status: 1,
      configuration_id: -1,
      ring_number: vec![1, 2, 3],
      node_id: None,
      metadata: None,
    });
    thread::sleep(Duration::from_secs(3));
    alerter.stop();
  }

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
