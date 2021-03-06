use crate::rmq::{Message, TimestampedMessage};
use chrono::prelude::*;
use failure::Error;
use futures::sync::mpsc::Receiver;
use futures::{future, Future, Stream};
use hyper::Client;
use opt::EsConfig as Config;
use serde_json;
use std::boxed::Box;
use std::sync::Arc;
use url::Url;

mod http;
pub mod query;

use self::query::MessageQuery;

pub type Task = Box<Future<Item = (), Error = Error> + Send>;
pub type IoFuture<A> = Box<Future<Item = A, Error = Error> + Send>;
pub type FilteredQuery = query::FilteredQuery;
pub type MessageQueryBuilder = query::MessageQueryBuilder;

#[derive(Serialize, Deserialize, Debug)]
pub struct EsCluster {
    pub version: EsVersion,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EsVersion {
    pub number: String,
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

impl From<Vec<(String, TimestampedMessage)>> for EsResult<TimestampedMessage> {
    fn from(msgs: Vec<(String, TimestampedMessage)>) -> Self {
        let hits = EsHits {
            total: msgs.len(),
            hits: msgs
                .into_iter()
                .map(|(id, msg)| EsDoc {
                    _id: id,
                    _source: msg,
                })
                .collect(),
        };
        EsResult {
            hits,
            aggregations: None,
        }
    }
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
            })
            .collect()
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

//TODO: simply return Url, handlying error internally
trait EsEndpoints {
    fn root_url(&self) -> Url;
    fn message_url(&self, id: Option<String>) -> Url;
    fn index_url(&self) -> Url;
    fn mapping_url(&self) -> Url;
    fn search_url(&self) -> Url;
    fn flush_url(&self) -> Url;
}

impl EsEndpoints for Config {
    fn root_url(&self) -> Url {
        Url::parse(&self.base_url).expect("Cannot parse root_url")
    }

    fn message_url(&self, id: Option<String>) -> Url {
        let without_id = self.index_url().join(&format!("{}/", self.message_type));

        let url = match id {
            Some(id) => without_id.and_then(|u| u.join(&format!("{}/", id))),
            _ => without_id,
        };
        url.expect("Couldn't parse message url")
    }

    fn index_url(&self) -> Url {
        self.root_url()
            .join(&format!("{}/", &self.index))
            .expect("Couldn't parse index url")
    }

    fn flush_url(&self) -> Url {
        self.index_url()
            .join("_flush")
            .expect("Cannot parse flush url")
    }

    fn mapping_url(&self) -> Url {
        self.index_url()
            .join(&format!("_mapping/{}/", &self.message_type))
            .expect("Couldn't parse mapping url")
    }

    fn search_url(&self) -> Url {
        self.index_url()
            .join("_search/")
            .expect("Couldn't parse search url")
    }
}

pub trait MessageSearchService {
    fn write(&self, rx: Receiver<TimestampedMessage>) -> Task;
    fn init_store(&self) -> Task;
    fn search(&self, query: MessageQuery) -> IoFuture<EsResult<TimestampedMessage>>;
}

#[derive(Clone)]
pub struct MessageStore {
    config: Arc<Config>,
    es_version: Option<u8>,
}

impl MessageStore {
    pub fn detecting_es_version(config: Config) -> IoFuture<Self> {
        let client = Client::new();

        let req = http::request::get(&config.root_url());

        Box::new(http::expect::<EsCluster>(&client, req).map(move |cluster| {
            let version_number = cluster.version.number;
            let mut major_min_bugfix = version_number.split('.');
            let es_version = match major_min_bugfix.next().map(|s| s.parse::<u8>().expect("expected es-major-version to be an integer")) {
                Some(n) if n == 2 || n == 6 => {
                    debug!("Supported Elasticsearch major version detected: {}", n);
                    Some(n)
                }
                _ => {
                    warn!("Unsupported Elasticsearch version detected. Supported versions are 2xx and 6xx. This is likely to fail!");
                    None
                }
            };

            MessageStore::new(config, es_version)
        }))
    }

    pub fn new(config: Config, es_version: Option<u8>) -> Self {
        MessageStore {
            config: Arc::new(config),
            es_version,
        }
    }
    fn create_index(&self) -> Task {
        let client = Client::new();
        let index_url = self.config.index_url();
        let mappings_url = self.config.mapping_url();
        let es_version = self.es_version;
        let es_field = move |field_type: &str| {
            if es_version == Some(2 as u8) {
                json!({
                    "type": field_type,
                    "index": "not_analyzed",
                })
            } else {
                let field_type = if field_type == "string" {
                    "keyword"
                } else {
                    field_type
                };

                json!({
                    "type": field_type,
                })
            }
        };

        let req = http::request::put::<()>(&index_url, None);

        Box::new(http::expect_ok(&client, req).and_then(move |_| {
            let mappings: serde_json::Value = json!({
              "properties": {
                  "replayed": es_field("boolean"),
                  "received_at": es_field("date"),
                  "redelivered": es_field("boolean"),
                  "routing_key": es_field("string"),
                  "exchange": es_field("string"),
                  "node": es_field("string"),
                  "routed_queues": es_field("string"),
                  "uuid": es_field("string"),
                  "properties": {
                      "type": "object",
                      "enabled": "false"
                  },
                  "headers": {
                      "type": "object",
                      "enabled": "false"
                  }
              }
            });

            let req = http::request::put::<serde_json::Value>(&mappings_url, Some(mappings));

            debug!("sending request: {:?}", req);

            http::expect_ok(&client, req)
        }))
    }
}

impl MessageSearchService for MessageStore {
    // NOTE: what happens when the mappings change?
    //
    // ES provides a convenient API for that: https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-reindex.html
    // perhaps this tool should automatically manage migrations by managing two indices at the same time...

    fn init_store(&self) -> Task {
        let client = Client::new();

        let req = http::request::get(&self.config.index_url());
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
        let ep_url = self.config.message_url(None);

        Box::new(
            rx.map_err(|_| format_err!("failed to receive message"))
                .and_then(move |msg| {
                    http::expect_ok(&Client::new(), http::request::post(&ep_url, Some(msg)))
                })
                .for_each(|_| Ok(())),
        )
    }

    fn search(&self, query: MessageQuery) -> IoFuture<EsResult<TimestampedMessage>> {
        let search_url = self.config.search_url();
        let config = self.config.clone();
        let client = Client::new();
        match query {
            MessageQuery::Ids(ids) => {
                let ids_and_urls = ids
                    .into_iter()
                    .map(move |id| (id.clone(), config.clone().message_url(Some(id))));
                Box::new(
                    future::join_all(ids_and_urls.map(move |(id, index_url)| {
                        let req = http::request::get(&index_url);
                        info!("sending a request: {:?}", req);

                        Box::new(
                            http::expect_option::<EsDoc<TimestampedMessage>>(&client, req)
                                .map(|maybe_doc| (id, maybe_doc.map(|doc| doc._source))),
                        )
                    }))
                    .map(|result| {
                        let found: Vec<(String, TimestampedMessage)> = result
                            .into_iter()
                            .filter_map(|(id, maybe_doc)| maybe_doc.map(|doc| (id, doc)))
                            .collect();
                        EsResult::<TimestampedMessage>::from(found)
                    }),
                )
            }
            MessageQuery::Filtered(fq) => {
                let json_query = fq.as_json(&self.es_version);
                info!(
                    "sending ES query: {} to {:?}",
                    serde_json::to_string_pretty(&json_query).unwrap(),
                    search_url
                );

                let req = http::request::post(&search_url, Some(json_query));
                Box::new(http::expect::<EsResult<TimestampedMessage>>(&client, req))
            }
        }
    }
}

#[cfg(test)]
pub mod test {
    extern crate env_logger;
    extern crate pretty_assertions;
    use crate::es::*;
    use crate::lapin::types::AMQPValue;
    use crate::rmq::{Message, Properties};
    use crate::TimeRange;
    use es::test::pretty_assertions::assert_eq;
    use futures::future;
    use futures::Future;
    use hyper::Client;
    use tokio::runtime::Runtime;

    fn reset_store(config: Config) -> IoFuture<MessageStore> {
        let client = Client::new();

        let head_req = http::request::head(&config.index_url());

        Box::new(http::is_ok(&client, head_req).and_then(move |initialised| {
            let clean = if initialised {
                let delete_req = http::request::delete(&config.index_url());
                http::expect_ok(&client, delete_req)
            } else {
                Box::new(future::lazy(|| Ok(())))
            };

            Box::new(
                clean
                    .and_then(move |_| MessageStore::detecting_es_version(config.clone()))
                    .and_then(|store| store.init_store().map(move |_| store)),
            )
        }))
    }

    //TODO: borrow config
    fn create_msgs(config: Config, msgs: Vec<StoredMessage>) -> IoFuture<MessageStore> {
        let flush_url = config.flush_url();

        Box::new(reset_store(config.clone()).and_then(|store| {
            debug!("creating test messages ...");

            let client = Client::new();
            future::join_all(msgs.into_iter().map(move |msg| {
                let msg_url = config.message_url(Some(msg.id.clone()));
                let msg: TimestampedMessage = msg.into();
                let req = http::request::put(&msg_url, Some(msg));
                http::expect_ok(&client, req)
            }))
            .and_then(move |_| {
                let client = Client::new();
                let req = http::request::post::<()>(&flush_url, None);
                http::expect_ok(&client, req)
            })
            .map(|_| store)
        }))
    }

    fn init_test_store(rt: &mut Runtime, msgs: Vec<StoredMessage>) -> MessageStore {
        let config = Config {
            index: "rabbit_messages_test".to_string(),
            message_type: "rabbit_message".to_string(),
            base_url: "http://localhost:9200".to_string(),
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
                        direction: None,
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
        q: FilteredQuery,
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
                .block_on(store.search(MessageQuery::Filtered(q)))
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
        env_logger::init();
        let in_store = vec![
            MessageBuilder::published_on("a", "exchange-2").build(),
            MessageBuilder::published_on("b", "exchange-1").build(),
            MessageBuilder::published_on("c", "exchange-1").build(),
        ];

        let q = MessageQueryBuilder::default()
            .with_exchange("exchange-1")
            .build();
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

        let q = MessageQueryBuilder::default()
            .with_exchange("exchange-1")
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

        let q = MessageQueryBuilder::default()
            .with_exchange("exchange-1")
            .with_time_range(TimeRange::Within(t2, t3))
            .build();

        assert_search_result_include(q, in_store, vec![1, 2]);
    }
}
