use std::net::{SocketAddr, ToSocketAddrs};
use futures::future::Future;
use futures::{Stream, Sink};
use futures::sync::mpsc::Sender;
use lapin::message::Delivery;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use lapin::client;
use lapin::client::ConnectionOptions;
use lapin::channel::{BasicConsumeOptions,QueueDeclareOptions, QueueBindOptions, ConfirmSelectOptions};
use lapin::types::FieldTable;
use tokio;
use serde_json;
use std::str::from_utf8;

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    pub routing_key: Option<String>,
    pub exchange: String,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub body: String
}

impl From<Delivery> for Message {
    fn from(d: Delivery) -> Self {
        Message {
          routing_key: Some(d.routing_key).filter(|key|!key.trim().is_empty()),
          exchange: d.exchange,
          delivery_tag: d.delivery_tag,
          redelivered: d.redelivered,
          body: from_utf8( &d.data).unwrap().to_string(),
        }
    }
}

pub struct Config {
    pub host: String,
    pub port: u16,
    pub exchange: String,
    pub queue_name: String,
}

impl Config {
    //TODO: just use a connection string
    fn address(&self) -> SocketAddr {
        let host_port = format!("{}:{}", self.host, self.port);
        host_port
            .to_socket_addrs()
            .expect(&format!("cannot resolve {}", host_port))
            .next()
            .unwrap()
    }
}


impl Default for Config {
    fn default() -> Self {
        Config {
            host: "127.0.0.1".to_string(),
            port: 5672,
            exchange: "amq.rabbitmq.trace".to_string(),
            queue_name: "rabbit-forwarder".to_string(),
        }
    }
}

pub fn bind_and_consume(config: Config, tx: Sender<Message>) {
    let queue_name = config.queue_name.clone();
    let exchange = config.exchange.clone();
    let addr = config.address();
    //TODO: can we reuse a runtime?
    let mut runtime = Runtime::new().unwrap();
    let mut tx = tx.clone().wait();

    runtime.block_on(
        TcpStream::connect(&addr).and_then(|stream| {
            // connect() returns a future of an AMQP Client
            // that resolves once the handshake is done
            client::Client::connect(stream, ConnectionOptions::default())
        }).and_then(|(client, hearthbeat)| {
            //TODO: spawn using the same runtime.
            tokio::spawn(
                hearthbeat.map_err(|e| eprintln!("The heartbeat task errored: {}", e))
            );
            client.create_confirm_channel(ConfirmSelectOptions::default())
        }).and_then(move |channel| {
            let id = channel.id;
            info!("created channel with id: {}", id);

            // we using a "move" closure to reuse the channel
            // once the queue is declared. We could also clone
            // the channel
            channel.queue_declare(&queue_name,
                                  QueueDeclareOptions::default(),
                                  FieldTable::default())
                .and_then(move |queue| {
                    println!("channel {} declared queue {}", id, "hello");
                    channel.queue_bind(&queue_name,
                                       &exchange,
                                       "#",
                                       QueueBindOptions::default(),
                                       FieldTable::default()).map(move |_| (channel, queue))
                        .and_then(move |(channel, queue)| {
                            println!("creating consumer");
                            channel.basic_consume(&queue,
                                                  "",
                                                  BasicConsumeOptions::default(),
                                                  FieldTable::new()).map(move |stream| (channel, stream))
                        }).and_then(move |(channel, stream)| {
                        stream.for_each(move |delivery| {
                            let tag = delivery.delivery_tag.clone();
                            let msg = Message::from(delivery);
                            let msg_json = serde_json::to_string(&msg).unwrap();
                            tx.send(msg).expect(&format!("failed to send message: {}", msg_json));
                            println!("got message: {}", msg_json);
                            channel.basic_ack(tag, false)
                        })
                    })
                })
        })
    ).expect("runtime failure");
}
