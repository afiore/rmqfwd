use crate::es::StoredMessage;
use crate::lapin::channel::Channel;
use crate::lapin::channel::{
    BasicConsumeOptions, BasicProperties, BasicPublishOptions, ConfirmSelectOptions,
    QueueBindOptions, QueueDeclareOptions,
};
use crate::lapin::client::Client;
use crate::lapin::client::ConnectionOptions;
use crate::lapin::message::Delivery;
use crate::lapin::types::*;
use chrono::prelude::*;
use failure::Error;
use futures::future::Future;
use futures::sync::mpsc::Sender;
use futures::{future, IntoFuture, Sink, Stream};
use opt::RmqConfig as Config;
use serde_json;
use std::collections::BTreeMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::str;
use std::str::FromStr;
use tokio;
use tokio::net::TcpStream;

const REPLAYED_HEADER: &str = "X-Replayed";

#[derive(Serialize, Deserialize, Debug)]
pub struct TimestampedMessage {
    pub received_at: DateTime<Utc>,
    pub replayed: bool,
    #[serde(flatten)]
    pub message: Message,
}

impl TimestampedMessage {
    pub fn now(msg: Message) -> TimestampedMessage {
        TimestampedMessage {
            replayed: msg.is_replayed(),
            received_at: Utc::now(),
            message: msg,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UserCreds {
    pub user: String,
    pub password: String,
}

impl FromStr for UserCreds {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let tokens: Vec<&str> = s.split(':').collect();

        if tokens.len() > 1 {
            Ok(UserCreds {
                user: tokens[0].to_string(),
                password: tokens[1].to_string(),
            })
        } else {
            Err(failure::format_err!(
                "expected creds in the following format: `user:password`. Got {}",
                s
            ))
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

impl Message {
    pub fn basic_properties(&self, replayed: bool) -> BasicProperties {
        let properties = self.properties.clone();
        let mut props = BasicProperties::default();
        let mut headers = amqp_field_table(&self.headers);

        if replayed {
            headers.insert(REPLAYED_HEADER.to_string(), AMQPValue::Boolean(true));
        }

        if !headers.is_empty() {
            props = props.with_headers(headers);
        }
        if let Some(content_type) = properties.content_type {
            props = props.with_content_type(content_type);
        }
        if let Some(content_encoding) = properties.content_encoding {
            props = props.with_content_encoding(content_encoding);
        }
        if let Some(delivery_mode) = properties.delivery_mode {
            props = props.with_delivery_mode(delivery_mode);
        }
        if let Some(correlation_id) = properties.correlation_id {
            props = props.with_correlation_id(correlation_id);
        }
        if let Some(reply_to) = properties.reply_to {
            props = props.with_reply_to(reply_to);
        }
        if let Some(expiration) = properties.expiration {
            props = props.with_expiration(expiration);
        }
        if let Some(message_id) = properties.message_id {
            props = props.with_message_id(message_id);
        }
        if let Some(timestamp) = properties.timestamp {
            props = props.with_timestamp(timestamp);
        }
        if let Some(user_id) = properties.user_id {
            props = props.with_user_id(user_id);
        }
        if let Some(app_id) = properties.app_id {
            props = props.with_app_id(app_id);
        }
        if let Some(cluster_id) = properties.cluster_id {
            props = props.with_cluster_id(cluster_id);
        }
        props
    }

    pub fn is_replayed(&self) -> bool {
        amqp_field_table(&self.headers).contains_key(REPLAYED_HEADER)
    }
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
            cluster_id: None,
        }
    }
}

fn amqp_str(v: &AMQPValue) -> Option<String> {
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

fn amqp_str_array(v: &AMQPValue) -> Vec<String> {
    match v {
        AMQPValue::FieldArray(vs) => vs.iter().filter_map(amqp_str).collect(),
        _ => Vec::new(),
    }
}

fn amqp_field_table(v: &AMQPValue) -> FieldTable {
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
            .and_then(|rk| amqp_str_array(&rk).into_iter().next());
        let routed_queues = headers
            .remove("routed_queues")
            .map(|q| amqp_str_array(&q))
            .unwrap_or_else(Vec::new);

        info!("message headers: {:#?}", headers);

        let mut props = headers
            .remove("properties")
            .map(|p| amqp_field_table(&p))
            .unwrap_or_else(BTreeMap::new);
        let prop_headers = props
            .remove("headers")
            .map(|p| amqp_field_table(&p))
            .unwrap_or_else(BTreeMap::new);
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
            routing_key,
            routed_queues,
            exchange: d.routing_key,
            redelivered: d.redelivered,
            body: str::from_utf8(&d.data).unwrap().to_string(),
            node,
            properties,
            headers: AMQPValue::FieldTable(prop_headers),
        }
    }
}

fn address(config: Config) -> SocketAddr {
    let host_port = format!("{}:{}", config.host, config.port);
    host_port
        .to_socket_addrs()
        .unwrap_or_else(|_| panic!("cannot resolve {}", host_port))
        .next()
        .unwrap()
}

fn tcp_connection(
    address: SocketAddr,
    opts: ConnectionOptions,
    attempts: u8,
) -> Box<Future<Item = Client<TcpStream>, Error = failure::Error> + Send + 'static> {
    let max_attempts = 10;
    let opts2 = opts.clone();
    Box::new(
        TcpStream::connect(&address)
            .map_err(Error::from)
            .and_then(move |stream| Client::connect(stream, opts).map_err(Error::from))
            .and_then(|(client, heartbeat)| {
                tokio::spawn(heartbeat.map_err(|e| eprintln!("heartbeat error: {:?}", e)))
                    .into_future()
                    .map(|_| client)
                    .map_err(|_| failure::err_msg("spawn error"))
            })
            .then(move |res| match res {
                Err(e) => {
                    if attempts > max_attempts {
                        Box::new(future::err(e))
                    } else {
                        // TODO: wrap sleep into a future
                        //
                        //use std::time::{Duration, Instant};
                        //let when = Instant::now() + Duration::from_secs(2);
                        //Box::new(
                        //    Delay::new(when)
                        //        .map_err(Error::from)
                        //        .and_then(|_| tcp_connection(address.clone(), attempts + 1)),
                        //)

                        warn!(
                            "failed to connect to rabbitmq. Re-attempting in {} seconds ...",
                            attempts
                        );
                        use std::time::Duration;
                        let wait_time = Duration::from_secs(u64::from(attempts));
                        std::thread::sleep(wait_time);
                        tcp_connection(address, opts2, attempts + 1)
                    }
                }
                Ok(stream) => Box::new(future::ok(stream)),
            }),
    )
}

fn setup_channel(
    config: Config,
) -> Box<Future<Item = Channel<TcpStream>, Error = failure::Error> + Send + 'static> {
    let defaults = ConnectionOptions::default();
    let (username, password) = config
        .creds
        .clone()
        .map(|c| (c.user, c.password))
        .unwrap_or((defaults.username, defaults.password));

    let connection_opts = ConnectionOptions {
        frame_max: 65535,
        heartbeat: 20,
        username,
        password,
        ..defaults
    };
    //TODO: do we have to convert the error at each step?
    Box::new(
        tcp_connection(address(config), connection_opts, 1).and_then(|client| {
            client
                .create_confirm_channel(ConfirmSelectOptions::default())
                .map_err(Error::from)
        }),
    )
}

pub fn publish<I>(
    config: Config,
    exchange: String,
    routing_key: Option<String>,
    stored_msgs: I,
) -> Box<Future<Item = (), Error = failure::Error> + Send + 'static>
where
    I: IntoIterator<Item = StoredMessage> + Send + 'static,
{
    Box::new(setup_channel(config).and_then(move |channel| {
        let routing_key = routing_key.unwrap_or_else(|| "#".to_string());

        future::join_all(stored_msgs.into_iter().map(move |stored| {
            debug!(
                "replaying message: {:#?} to exchange: {:?} with routing key: {:?}",
                stored, exchange, routing_key
            );
            let is_replayed = true;
            let basic_properties = stored.message.basic_properties(is_replayed);
            let msg_body = stored.message.body.into_bytes();
            channel
                .basic_publish(
                    &exchange,
                    &routing_key,
                    msg_body,
                    BasicPublishOptions::default(),
                    basic_properties,
                )
                .map(|confirmation| {
                    debug!("got confirmation of publication: {:?}", confirmation);
                })
        }))
        .map_err(Error::from)
        .map(|_| ())
    }))
}

pub fn bind_and_consume(
    config: Config,
    tx: Sender<TimestampedMessage>,
) -> Box<Future<Item = (), Error = failure::Error> + Send> {
    let mut tx = tx.clone().wait();
    let queue_name = "rmqfwd"; //TODO: make configurable
    let exchange = config.tracing_exchange.clone();

    Box::new(setup_channel(config).and_then(move |channel| {
        let id = channel.id;
        info!("created channel with id: {}", id);

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
                            let tag = delivery.delivery_tag;
                            debug!("got message: {:?}", delivery);
                            let msg = Message::from(delivery);
                            let msg_json = serde_json::to_string(&msg).unwrap();
                            tx.send(TimestampedMessage::now(msg)).unwrap_or_else(|_| {
                                panic!("failed to send message through channel: {}", msg_json)
                            });
                            info!("got message: {}", msg_json);
                            channel.basic_ack(tag, false)
                        })
                    })
            })
            .map_err(Error::from)
    }))
}
