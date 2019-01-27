use chrono::prelude::*;
use clap::ArgMatches;
use es::StoredMessage;
use failure::Error;
use futures::future::Future;
use futures::sync::mpsc::Sender;
use futures::{future, IntoFuture, Sink, Stream};
use lapin::channel::Channel;
use lapin::channel::{
    BasicConsumeOptions, BasicProperties, BasicPublishOptions, ConfirmSelectOptions,
    QueueBindOptions, QueueDeclareOptions,
};
use lapin::client;
use lapin::client::ConnectionOptions;
use lapin::message::Delivery;
use lapin::types::*;
use serde_json;
use std::collections::BTreeMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::str;
use tokio;
use tokio::net::TcpStream;

const REPLAYED_HEADER: &str = "X-Replayed";

#[derive(Serialize, Deserialize, Debug)]
pub struct TimestampedMessage {
    pub received_at: DateTime<Utc>,
    pub replayed: bool,
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
        for content_type in properties.content_type {
            props = props.with_content_type(content_type);
        }
        for content_encoding in properties.content_encoding {
            props = props.with_content_encoding(content_encoding);
        }
        for delivery_mode in properties.delivery_mode {
            props = props.with_delivery_mode(delivery_mode);
        }
        for correlation_id in properties.correlation_id {
            props = props.with_correlation_id(correlation_id);
        }
        for reply_to in properties.reply_to {
            props = props.with_reply_to(reply_to);
        }
        for expiration in properties.expiration {
            props = props.with_expiration(expiration);
        }
        for message_id in properties.message_id {
            props = props.with_message_id(message_id);
        }
        for timestamp in properties.timestamp {
            props = props.with_timestamp(timestamp);
        }
        for user_id in properties.user_id {
            props = props.with_user_id(user_id);
        }
        for app_id in properties.app_id {
            props = props.with_app_id(app_id);
        }
        for cluster_id in properties.cluster_id {
            props = props.with_cluster_id(cluster_id);
        }
        props
    }

    pub fn is_replayed(&self) -> bool {
        let headers = amqp_field_table(&self.headers);
        headers.contains_key(REPLAYED_HEADER)
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
            .and_then(|rk| amqp_str_array(rk).into_iter().next());
        let routed_queues = headers
            .remove("routed_queues")
            .map(amqp_str_array)
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

#[derive(Clone)]
pub struct Creds {
    pub user: String,
    pub password: String,
}

pub struct Config {
    pub host: String,
    pub port: u16,
    pub exchange: String,
    pub queue_name: String,
    pub creds: Option<Creds>,
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

impl<'a, 'b> From<&'a ArgMatches<'b>> for Config {
    fn from(matches: &'a ArgMatches<'b>) -> Config {
        let mut config = Config::default();

        if let Some(host) = matches.value_of("rmq-host") {
            config.host = host.to_string();
        }

        if let Some(port) = matches.value_of("rmq-port") {
            config.port = port
                .parse::<u16>()
                .expect("rmq-port: postive integer expected!")
        }

        if let Some(exchange) = matches.value_of("rmq-exchange") {
            config.exchange = exchange.to_string();
        }

        if let Some(rmq_creds) = matches.value_of("rmq-creds") {
            let mut creds_tokens = rmq_creds.split(":").into_iter();

            if let Some((user, pass)) = creds_tokens
                .next()
                .and_then(|user| creds_tokens.next().map(|pass| (user, pass)))
            {
                config.creds = Some(Creds {
                    user: user.to_string(),
                    password: pass.to_string(),
                });
            }
        }

        config
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: "127.0.0.1".to_string(),
            port: 5672,
            exchange: "amq.rabbitmq.trace".to_string(),
            queue_name: "rabbit-forwarder".to_string(),
            creds: None,
        }
    }
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
        username: username,
        password: password,
        ..defaults
    };
    //TODO: do we have to convert the error at each step?
    Box::new(
        TcpStream::connect(&config.address())
            .map_err(Error::from)
            .and_then(|stream| {
                client::Client::connect(stream, connection_opts).map_err(Error::from)
            })
            .and_then(|(client, heartbeat)| {
                tokio::spawn(heartbeat.map_err(|e| eprintln!("heartbeat error: {:?}", e)))
                    .into_future()
                    .map(|_| client)
                    .map_err(|_| failure::err_msg("spawn error"))
            })
            .and_then(|client| {
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
        let routing_key = routing_key.unwrap_or("#".to_string());

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
    let queue_name = config.queue_name.clone();
    let exchange = config.exchange.clone();

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
            .map_err(Error::from)
    }))
}
