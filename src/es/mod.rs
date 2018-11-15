use chrono::prelude::*;
use clap::ArgMatches;
use failure::Error;
use futures::stream;
use futures::sync::mpsc::Receiver;
use futures::{Future, Stream};
use hyper::Body;
use hyper::Client;
use hyper::Method;
use hyper::Request;
use rmq::{Message, TimestampedMessage};
use serde_json;
use serde_json::Value;
use std::boxed::Box;
use std::sync::Arc;
use url::{ParseError, Url};

mod http;
mod query;

pub type Task = Box<Future<Item = (), Error = Error> + Send>;
pub type IoFuture<A> = Box<Future<Item = A, Error = Error> + Send>;
pub type MessageQuery = query::MessageQuery;
pub type MessageQueryBuilder = query::MessageQueryBuilder;

#[derive(Debug, Clone)]
pub struct Config {
    //TODO: add support for TLS
    base_url: String,
    index: String,
    doc_type: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            base_url: "http://localhost:9200".to_string(),
            index: "rabbit_messages".to_string(),
            doc_type: "message".to_string(),
        }
    }
}

impl<'a, 'b> From<&'a ArgMatches<'b>> for Config {
    fn from(matches: &'a ArgMatches<'b>) -> Config {
        let default = Config::default();
        let base_url = matches
            .value_of("es-base-url")
            .map(|s| s.to_string())
            .unwrap_or_else(|| default.base_url.clone());

        let index = matches
            .value_of("es-index")
            .map(|s| s.to_string())
            .unwrap_or_else(|| default.index.clone());

        let doc_type = matches
            .value_of("es-type")
            .map(|s| s.to_string())
            .unwrap_or_else(|| default.doc_type.clone());

        Config {
            base_url: base_url,
            index: index,
            doc_type: doc_type,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EsDoc<A> {
    pub _source: A,
    pub _id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EsHits<A> {
    pub total: usize,
    pub hits: Vec<EsDoc<A>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EsBucket {
    pub key: String,
    pub doc_count: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EsAggregation {
    pub buckets: Vec<EsBucket>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EsMessageAgg {
    pub exchange: EsAggregation,
    pub routing_key: EsAggregation,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EsAggregations {
    pub message: EsMessageAgg,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EsResult<A> {
    pub hits: EsHits<A>,
    pub aggregations: Option<EsAggregations>,
}

impl Into<Vec<StoredMessage>> for EsResult<TimestampedMessage> {
    fn into(self) -> Vec<StoredMessage> {
        info!("found {} results", self.hits.total);
        self.hits
            .hits
            .into_iter()
            .map(|es_doc| StoredMessage {
                id: es_doc._id,
                received_at: es_doc._source.received_at,
                replayed: es_doc._source.replayed,
                message: es_doc._source.message,
            }).collect()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct StoredMessage {
    pub received_at: DateTime<Utc>,
    pub message: Message,
    pub replayed: bool,
    pub id: String,
}

impl Into<TimestampedMessage> for StoredMessage {
    fn into(self) -> TimestampedMessage {
        TimestampedMessage {
            received_at: self.received_at,
            message: self.message,
            replayed: self.replayed,
        }
    }
}

trait EsEndpoints {
    fn message_url(&self, id: Option<String>) -> Result<Url, ParseError>;
    fn index_url(&self) -> Result<Url, ParseError>;
    fn mapping_url(&self) -> Result<Url, ParseError>;
    fn search_url(&self) -> Result<Url, ParseError>;
    fn flush_url(&self) -> Result<Url, ParseError>;
}

impl EsEndpoints for Config {
    fn message_url(&self, id: Option<String>) -> Result<Url, ParseError> {
        let without_id = self
            .index_url()
            .and_then(|u| u.join(&format!("{}/", self.doc_type)))?;

        match id {
            Some(id) => without_id.join(&format!("{}/", id)),
            _ => Ok(without_id),
        }
    }

    fn index_url(&self) -> Result<Url, ParseError> {
        Url::parse(&self.base_url).and_then(|u| u.join(&format!("{}/", &self.index)))
    }

    fn flush_url(&self) -> Result<Url, ParseError> {
        let index_url = self.index_url()?;
        let flush_url = index_url.join("_flush")?;
        Ok(flush_url)
    }

    fn mapping_url(&self) -> Result<Url, ParseError> {
        let index_url = self.index_url()?;
        let mapping_url = index_url.join(&format!("_mapping/{}/", &self.doc_type))?;
        Ok(mapping_url)
    }

    fn search_url(&self) -> Result<Url, ParseError> {
        let index_url = self.index_url()?;
        let search_url = index_url.join("_search/")?;
        Ok(search_url)
    }
}

pub trait MessageSearchService {
    fn write(&self, rx: Receiver<TimestampedMessage>) -> Task;
    fn init_store(&self) -> Task;
    fn search(&self, query: query::MessageQuery) -> IoFuture<EsResult<TimestampedMessage>>;

    //NOTE: this might be obsolete
    fn message_for(&self, id: String) -> IoFuture<Option<StoredMessage>>;
}

#[derive(Clone)]
pub struct MessageStore {
    config: Arc<Config>,
}

impl MessageStore {
    pub fn new(config: Config) -> Self {
        MessageStore {
            config: Arc::new(config),
        }
    }
    fn create_index(&self) -> Task {
        let client = Client::new();
        let index_url = self.config.index_url().unwrap();
        let mappings_url = self.config.mapping_url().unwrap();

        let req = Request::builder()
            .method(Method::POST)
            .uri(index_url.to_string())
            .body(Body::empty())
            .expect("couldn't build a request!");

        Box::new(http::expect_ok(&client, req).and_then(move |_| {
            let mappings: serde_json::Value = json!({
                "properties": {
                    "replayed": {
                        "type": "boolean",
                        "index": "not_analyzed"
                    },
                    "received_at": {
                        "type": "date",
                        "index": "not_analyzed"
                    },
                    "message": {
                        "type": "nested",
                        "properties": {
                            "exchange": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "routing_key": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "redelivered": {
                                "type": "boolean",
                                "index": "not_analyzed"
                            },
                            "uuid": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "headers": {
                                "type": "object",
                                "enabled": "false"
                            }
                        }
                    }
                }
              });

            let req = Request::builder()
                .method(Method::PUT)
                .uri(mappings_url.to_string())
                .body(Body::wrap_stream(stream::once(serde_json::to_string(
                    &mappings,
                )))).expect("couldn't build a request!");

            debug!("sending request: {:?}", req);

            http::expect_ok(&client, req)
        }))
    }
}

impl MessageSearchService for MessageStore {
    // NOTE: what happens when the mappings change?
    // ES provides a convenient API for that: https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-reindex.html
    // perhaps this tool should automatically manage migrations by managing two indices at the same time...
    //
    // TODO: do not create the index if it already exists!
    //

    fn init_store(&self) -> Task {
        use futures::future;
        let client = Client::new();
        let index_url = self.config.index_url().unwrap();

        let req = Request::builder()
            .method(Method::HEAD)
            .uri(index_url.to_string())
            .body(Body::empty())
            .expect("couldn't build a request!");

        let create_index = self.create_index();

        Box::new(http::is_ok(&client, req).and_then(|initalised| {
            if initalised {
                info!("store already initialised. Skipping index creation");
                Box::new(future::ok::<(), Error>(()))
            } else {
                create_index
            }
        }))
    }

    fn write(&self, rx: Receiver<TimestampedMessage>) -> Task {
        //TODO: handle error
        let ep_url = self.config.message_url(None).unwrap().to_string();

        Box::new(
            rx.map_err(|_| format_err!("failed to receive message"))
                .and_then(move |msg| {
                    let client = Client::new();
                    let body = Body::wrap_stream(stream::once(serde_json::to_string(&msg)));
                    let mut req = Request::builder();

                    req.method(Method::POST).uri(ep_url.clone());
                    http::expect_ok(
                        &client,
                        req.body(body)
                            .expect(&format!("couldn't build HTTP request {:?}", msg)),
                    )
                }).for_each(|_| Ok(())),
        )
    }

    fn message_for(&self, id: String) -> IoFuture<Option<StoredMessage>> {
        let client = Client::new();
        let index_url = self.config.message_url(Some(id)).unwrap();
        let req = Request::builder()
            .method(Method::GET)
            .uri(index_url.to_string())
            .body(Body::empty())
            .expect("couldn't build a request!");

        Box::new(
            http::expect_option::<EsDoc<StoredMessage>>(&client, req)
                .map(|maybe_doc| maybe_doc.map(|doc| doc._source)),
        )
    }

    fn search(&self, query: query::MessageQuery) -> IoFuture<EsResult<TimestampedMessage>> {
        let client = Client::new();
        let search_url = self.config.search_url().unwrap();
        let json_query: Value = query.into();
        info!(
            "sending ES query: {} to {:?}",
            serde_json::to_string_pretty(&json_query).unwrap(),
            search_url
        );

        let body = Body::wrap_stream(stream::once(serde_json::to_string(&json_query)));
        let req = Request::builder()
            .method(Method::POST)
            .uri(search_url.to_string())
            .body(body)
            .expect("couldn't build a request!");

        Box::new(http::expect::<EsResult<TimestampedMessage>>(&client, req))
    }
}

#[cfg(test)]
pub mod test {
    extern crate env_logger;
    use chrono::prelude::*;
    use es::*;
    use futures::Future;
    use futures::{future, stream};
    use hyper::{Body, Client, Method, Request};
    use lapin::types::AMQPValue;
    use rmq::{Message, Properties};
    use serde_json;
    use tokio::runtime::Runtime;
    use TimeRange;

    fn reset_store(config: Config) -> IoFuture<MessageStore> {
        let client = Client::new();
        let store = MessageStore::new(config.clone());
        let index_url = config.index_url().unwrap().to_string();

        let head_req = Request::builder()
            .method(Method::HEAD)
            .uri(index_url.to_string())
            .body(Body::empty())
            .expect("couldn't build a request!");

        Box::new(http::is_ok(&client, head_req).and_then(move |initialised| {
            let clean = if initialised {
                let delete_req = Request::builder()
                    .uri(index_url)
                    .method(Method::DELETE)
                    .body(Body::empty())
                    .expect("couldn't build DELETE request");

                debug!("deleting store ...");
                http::expect_ok(&client, delete_req)
            } else {
                Box::new(future::lazy(|| Ok(())))
            };

            Box::new(clean.and_then(move |_| store.init_store().map(move |_| store)))
        }))
    }

    //TODO: borrow config
    fn create_msgs(config: Config, msgs: Vec<StoredMessage>) -> IoFuture<MessageStore> {
        let flush_url = config.flush_url().unwrap();

        Box::new(reset_store(config.clone()).and_then(|store| {
            debug!("creating test messages ...");

            let client = Client::new();
            future::join_all(msgs.into_iter().map(move |msg| {
                let msg_url = config.message_url(Some(msg.id.clone())).unwrap();
                let msg: TimestampedMessage = msg.into();
                let msg_json = Body::wrap_stream(stream::once(serde_json::to_string_pretty(&msg)));
                let req = Request::builder()
                    .uri(msg_url.to_string())
                    .method(Method::PUT)
                    .body(msg_json)
                    .expect("couldn't build a request!");

                http::expect_ok(&client, req)
            })).and_then(move |_| {
                let client = Client::new();
                let req = Request::builder()
                    .uri(flush_url.to_string())
                    .method(Method::POST)
                    .body(Body::empty())
                    .expect("couldn't build a request!");

                http::expect_ok(&client, req)
            }).map(|_| store)
        }))
    }

    //TODO: borrow messages
    fn init_test_store(rt: &mut Runtime, msgs: Vec<StoredMessage>) -> MessageStore {
        let config = Config {
            index: "rabbit_messages_test".to_string(),
            ..Config::default()
        };

        let result = rt.block_on(create_msgs(config, msgs));

        match result {
            Ok(store) => store,
            Err(e) => panic!("Couldn't initialise test message store: {:?}", e),
        }
    }

    pub struct MessageBuilder {
        inner: StoredMessage,
    }

    impl MessageBuilder {
        pub fn published_on(id: &str, exchange: &str) -> Self {
            MessageBuilder {
                inner: StoredMessage {
                    id: id.to_owned(),
                    replayed: false,
                    received_at: Utc::now(),
                    message: Message {
                        routing_key: None,
                        exchange: exchange.to_owned(),
                        redelivered: false,
                        body: format!("message with id: {}", id).to_string(),
                        headers: AMQPValue::Void,
                        properties: Properties::default(),
                        node: None,
                        routed_queues: Vec::new(),
                    },
                },
            }
        }

        pub fn build(self) -> StoredMessage {
            self.inner
        }

        pub fn _as_replayed(mut self) -> Self {
            self.inner.replayed = true;
            self
        }

        pub fn received_at(mut self, at: DateTime<Utc>) -> Self {
            self.inner.received_at = at;
            self
        }

        pub fn with_body(mut self, body: &str) -> Self {
            self.inner.message.body = body.to_owned();
            self
        }

        pub fn _routed_to_queue(mut self, queue: String) -> Self {
            {
                let ref mut routed_queues = self.inner.message.routed_queues;
                if !routed_queues.contains(&queue) {
                    routed_queues.push(queue);
                }
            }
            self
        }

        pub fn with_routing_key(mut self, key: &str) -> Self {
            self.inner.message.routing_key = Some(key.to_owned());
            self
        }
    }

    fn assert_search_result_include(
        q: MessageQuery,
        in_store: Vec<StoredMessage>,
        expected_idx: Vec<usize>,
    ) {
        let mut rt = Runtime::new().unwrap();
        let mut msgs_expected = Vec::new();
        for idx in expected_idx {
            msgs_expected.push(in_store[idx].message.clone());
        }

        let store = init_test_store(&mut rt, in_store);

        let mut msgs_found: Vec<Message> = {
            let es_result = rt
                .block_on(store.search(q))
                .expect("search result expected");
            let stored_msgs: Vec<StoredMessage> = es_result.into();

            stored_msgs
                .into_iter()
                .map(|stored| stored.message)
                .collect()
        };

        msgs_found.sort_by(|a, b| a.body.cmp(&b.body));
        msgs_expected.sort_by(|a, b| a.body.cmp(&b.body));
        assert_eq!(msgs_expected, msgs_found);
    }

    #[test]
    fn filter_by_exchange_works() {
        let in_store = vec![
            MessageBuilder::published_on("a", "exchange-2").build(),
            MessageBuilder::published_on("b", "exchange-1").build(),
            MessageBuilder::published_on("c", "exchange-1").build(),
        ];

        let q = MessageQueryBuilder::with_exchange("exchange-1").build();
        assert_search_result_include(q, in_store, vec![1, 2]);
    }

    #[test]
    fn filter_by_exchange_routing_key_works() {
        let in_store = vec![
            MessageBuilder::published_on("a", "exchange-2").build(),
            MessageBuilder::published_on("b", "exchange-1")
                .with_routing_key("test-key")
                .build(),
            MessageBuilder::published_on("c", "exchange-1").build(),
        ];

        let q = MessageQueryBuilder::with_exchange("exchange-1")
            .with_routing_key("test-key")
            .build();

        assert_search_result_include(q, in_store, vec![1]);
    }

    #[test]
    fn filter_by_time_range() {
        use chrono::Duration;
        let t1 = Utc::now() - Duration::hours(2);
        let t2 = Utc::now();
        let t3 = Utc::now() + Duration::hours(1) + Duration::minutes(5);
        let t4 = Utc::now() + Duration::days(1);

        let in_store = vec![
            MessageBuilder::published_on("a", "exchange-1")
                .received_at(t1)
                .build(),
            MessageBuilder::published_on("b", "exchange-1")
                .received_at(t2)
                .build(),
            MessageBuilder::published_on("c", "exchange-1")
                .received_at(t3)
                .build(),
            MessageBuilder::published_on("d", "exchange-1")
                .received_at(t4)
                .build(),
        ];

        let q = MessageQueryBuilder::with_exchange("exchange-1")
            .with_time_range(TimeRange::Within(t2, t3))
            .build();

        assert_search_result_include(q, in_store, vec![1, 2]);
    }
}
