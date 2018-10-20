use chrono::prelude::*;
use futures::future::Future;
use futures::sync::mpsc::Sender;
use futures::{Sink, Stream};
use lapin::channel::{
    BasicConsumeOptions, ConfirmSelectOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::client;
use lapin::client::ConnectionOptions;
use lapin::message::Delivery;
use lapin::types::*;
use serde_json;
use std::collections::{BTreeMap};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::str;
use tokio;
use tokio::net::TcpStream;

#[derive(Serialize, Deserialize, Debug)]
pub struct TimestampedMessage {
  pub received_at: DateTime<Utc>,
  pub message: Message
}

impl TimestampedMessage {
    pub fn now(msg: Message) -> TimestampedMessage {
       TimestampedMessage {
         message: msg,
         received_at: Utc::now(),
       }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Message {
    pub routing_key: Option<String>,
    pub exchange: String,
    pub redelivered: bool,
    pub body: String,
    pub headers: AMQPValue,
    pub properties: Properties,
    pub node: Option<String>,
    pub routed_queues: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Properties {
    //TODO: add _type?
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    pub cluster_id: Option<String>,
}

impl Default for Properties {
    fn default() -> Self {
        Properties {
            content_type: None,
            content_encoding: None,
            delivery_mode: None,
            priority: None,
            correlation_id: None,
            reply_to: None,
            expiration: None,
            message_id: None,
            timestamp: None,
            user_id: None,
            app_id: None,
            cluster_id: None
        }
    }
}



fn amqp_str(ref v: &AMQPValue) -> Option<String> {
    match v {
        AMQPValue::LongString(s) => Some(s.to_string()),
        _ => None,
    }
}

fn amqp_u8(v: &AMQPValue) -> Option<u8> {
     match v {
        AMQPValue::ShortShortUInt(s) => Some(*s),
        _ => None,
    }
}

fn amqp_u64(v: &AMQPValue) -> Option<u64> {
     match v {
        AMQPValue::Timestamp(s) => Some(*s),
        _ => None,
    }
}

fn amqp_str_array(ref v: AMQPValue) -> Vec<String> {
    match v {
        AMQPValue::FieldArray(vs) => vs.into_iter().filter_map(amqp_str).collect(),
        _ => Vec::new(),
    }
}

fn amqp_field_table(ref v: AMQPValue) -> FieldTable {
    match v {
        AMQPValue::FieldTable(t) => t.clone(),
        _ => BTreeMap::new(),
    }
}

impl From<Delivery> for Message {
    fn from(d: Delivery) -> Self {
        let p = d.properties;
        let mut headers = p.headers().clone().unwrap_or_else(BTreeMap::new);
        let node = headers.remove("node").and_then(|n| amqp_str(&n));
        let routing_key = headers
            .remove("routing_keys")
            .and_then(|rk| amqp_str_array(rk).into_iter().next());
        let routed_queues = headers
            .remove("routed_queues")
            .map(amqp_str_array)
            .unwrap_or_else(Vec::new);

        let mut props = headers.remove("properties").map(amqp_field_table).unwrap_or_else(BTreeMap::new);
        let prop_headers = props.remove("headers").map(amqp_field_table).unwrap_or_else(BTreeMap::new);
        let properties = Properties {
            content_type: props.get("content_type").and_then(amqp_str),
            content_encoding: props.get("content_encoding").and_then(amqp_str),
            delivery_mode: props.get("delivery_mode").and_then(amqp_u8),
            priority: props.get("priority").and_then(amqp_u8),
            correlation_id: props.get("correlation_id").and_then(amqp_str),
            reply_to: props.get("reply_to").and_then(amqp_str),
            expiration: props.get("expiration").and_then(amqp_str),
            message_id: props.get("message_id").and_then(amqp_str),
            timestamp: props.get("timestamp").and_then(amqp_u64),
            user_id: props.get("user_id").and_then(amqp_str),
            app_id: props.get("app_id").and_then(amqp_str),
            cluster_id: props.get("app_id").and_then(amqp_str),
        };

        Message {
            routing_key: routing_key,
            routed_queues: routed_queues,
            exchange: d.routing_key,
            redelivered: d.redelivered,
            body: str::from_utf8(&d.data).unwrap().to_string(),
            node: node,
            properties: properties,
            headers: AMQPValue::FieldTable(prop_headers),
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

pub fn bind_and_consume(
    config: Config,
    tx: Sender<TimestampedMessage>,
) -> Box<Future<Item = (), Error = io::Error> + Send> {
    let queue_name = config.queue_name.clone();
    let exchange = config.exchange.clone();
    let addr = config.address();
    //TODO: can we reuse a runtime?
    let mut tx = tx.clone().wait();

    Box::new(TcpStream::connect(&addr)
        .and_then(|stream| {
            // connect() returns a future of an AMQP Client
            // that resolves once the handshake is done
            client::Client::connect(stream, ConnectionOptions::default())
        })
        .and_then(|(client, hearthbeat)| {
            //TODO: spawn using the same runtime.
            tokio::spawn(hearthbeat.map_err(|e| error!("The heartbeat task errored: {}", e)));
            client.create_confirm_channel(ConfirmSelectOptions::default())
        })
        .and_then(move |channel| {
            let id = channel.id;
            info!("created channel with id: {}", id);

            // we using a "move" closure to reuse the channel
            // once the queue is declared. We could also clone
            // the channel
            channel
                .queue_declare(
                    &queue_name,
                    QueueDeclareOptions::default(),
                    FieldTable::default(),
                )
                .and_then(move |queue| {
                    info!("channel {} declared queue {}", id, &queue_name);
                    channel
                        .queue_bind(
                            &queue_name,
                            &exchange,
                            "#",
                            QueueBindOptions::default(),
                            FieldTable::default(),
                        )
                        .map(move |_| (channel, queue))
                        .and_then(move |(channel, queue)| {
                            info!("creating consumer");
                            channel
                                .basic_consume(
                                    &queue,
                                    "",
                                    BasicConsumeOptions::default(),
                                    FieldTable::new(),
                                )
                                .map(move |stream| (channel, stream))
                        })
                        .and_then(move |(channel, stream)| {
                            stream.for_each(move |delivery| {
                                let tag = delivery.delivery_tag.clone();
                                debug!("got message: {:?}", delivery);
                                let msg = Message::from(delivery);
                                let msg_json = serde_json::to_string(&msg).unwrap();
                                tx.send(TimestampedMessage::now(msg)).expect(&format!(
                                    "failed to send message through channel: {}",
                                    msg_json
                                ));
                                info!("got message: {}", msg_json);
                                channel.basic_ack(tag, false)
                            })
                        })
                })
        }))
}
