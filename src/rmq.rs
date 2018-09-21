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
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    pub routing_key: Option<String>,
    pub exchange: String,
    pub redelivered: bool,
    pub body: String,
    pub headers: FieldTable,
    pub node: Option<String>,
    pub routed_queues: Vec<String>,
    pub received_at: DateTime<Utc>,
    pub uuid: Uuid,
}


fn amqp_str(ref v: &AMQPValue) -> Option<String> {
    match v {
        AMQPValue::LongString(s) => Some(s.to_string()),
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


#[derive(Serialize, Deserialize)]
#[serde(remote = "AMQPValue", tag = "amqp_tag", content = "value")]
pub enum AMQPValueDef {
    /// A bool
    Boolean(bool),
    /// An i8
    ShortShortInt(i8),
    /// A u8
    ShortShortUInt(u8),
    /// An i16
    ShortInt(i16),
    /// A u16
    ShortUInt(u16),
    /// An i32
    LongInt(i32),
    /// A u32
    LongUInt(u32),
    /// An i64
    LongLongInt(i64),
    /// An f32
    Float(f32),
    /// An f64
    Double(f64),
    /// A decimal value
    DecimalValue(DecimalValue),
    /// A String
    LongString(String),
    /// An array of AMQPValue
    FieldArray(FieldArray),
    /// A timestamp (u32)
    Timestamp(u64),
    /// A Map<String, AMQPValue>
    FieldTable(FieldTable),
    /// An array of bytes (RabbitMQ speicific)
    ByteArray(ByteArray),
    /// No value
    Void,
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

        Message {
            routing_key: routing_key,
            routed_queues: routed_queues,
            exchange: d.routing_key, //e.g. publish.exchange_name
            redelivered: d.redelivered,
            body: str::from_utf8(&d.data).unwrap().to_string(),
            node: node,
            headers: prop_headers,
            //TODO: introduce a wrapper type for these
            received_at: Utc::now(),
            uuid: Uuid::new_v4(),
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

//NOTE: couldn't return a Box<Future<...>>, as compiler complained about 'static lifetime
pub fn bind_and_consume(
    config: Config,
    tx: Sender<Message>,
) -> impl Future<Item = (), Error = io::Error> {
    let queue_name = config.queue_name.clone();
    let exchange = config.exchange.clone();
    let addr = config.address();
    //TODO: can we reuse a runtime?
    let mut tx = tx.clone().wait();

    TcpStream::connect(&addr)
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
                                tx.send(msg).expect(&format!(
                                    "failed to send message through channel: {}",
                                    msg_json
                                ));
                                info!("got message: {}", msg_json);
                                channel.basic_ack(tag, false)
                            })
                        })
                })
        })
}
