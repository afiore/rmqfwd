use chrono::prelude::*;
use failure::Error;
use futures::stream;
use futures::sync::mpsc::Receiver;
use futures::{Future, Stream};
use hyper::client::HttpConnector;
use hyper::Body;
use hyper::Chunk;
use hyper::Client;
use hyper::Method;
use hyper::Request;
use hyper::StatusCode;
use rmq::{Message, TimestampedMessage};
use serde::de::DeserializeOwned;
use serde_json;
use serde_json::Value;
use std::boxed::Box;
use std::sync::Arc;
use url::{ParseError, Url};
use TimeRange;

pub type Task = Box<Future<Item = (), Error = Error> + Send>;
pub type IoFuture<A> = Box<Future<Item = A, Error = Error> + Send>;

#[derive(Debug)]
pub struct Config {
    //TODO: add support for TLS
    base_url: String,
    index: String,
    doc_type: String,
}

#[derive(Deserialize, Debug)]
pub struct EsDoc<A> {
    pub _source: A,
    pub _id: String,
}

#[derive(Deserialize, Debug)]
pub struct EsHits<A> {
    pub hits: Vec<EsDoc<A>>,
}

#[derive(Deserialize, Debug)]
pub struct EsResult<A> {
    pub hits: EsHits<A>,
}

impl Into<Vec<StoredMessage>> for EsResult<TimestampedMessage> {
    fn into(self) -> Vec<StoredMessage> {
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

trait EsEndpoints {
    fn message_url(&self, id: Option<String>) -> Result<Url, ParseError>;
    fn index_url(&self) -> Result<Url, ParseError>;
    fn mapping_url(&self) -> Result<Url, ParseError>;
    fn search_url(&self) -> Result<Url, ParseError>;
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

impl Default for Config {
    fn default() -> Self {
        Config {
            base_url: "http://localhost:9200".to_string(),
            index: "rabbit_messages".to_string(),
            doc_type: "message".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct MessageQuery {
    pub exchange: String,
    pub body: Option<String>,
    pub routing_key: Option<String>,
    //TODO: parse from cli
    pub time_range: Option<TimeRange>,
    pub exclude_replayed: bool,
}

impl Into<Value> for MessageQuery {
    fn into(self) -> Value {
        let mut nested: Vec<Value> = Vec::new();

        nested.push(json!(
            {"match": {"message.exchange": self.exchange }}
        ));

        for key in self.routing_key {
            nested.push(json!({
              "match": {"message.routing-key": key }
            }));
        }
        for body in self.body {
            nested.push(json!({
              "match": {"message.body": body }
            }));
        }

        let nested_obj = json!({
            "nested": {
                "path": "message",
                "score_mode": "avg",
                "query": {
                    "bool": {
                        "must": nested
                    }
                }
        }});

        let filters = vec![
            nested_obj,
            json!({
                "query": {
                    "match": {
                        "replayed": !self.exclude_replayed
                    }
                }
            }),
        ];

        json!({
            "query": {
                 "bool": {
                     "must": filters
                 }
            }
        })
    }
}

pub trait MessageSearchService {
    fn write(&self, rx: Receiver<TimestampedMessage>) -> Task;
    fn init_store(&self) -> Task;
    fn search(&self, query: MessageQuery) -> IoFuture<Vec<StoredMessage>>;
    fn message_for(&self, id: String) -> IoFuture<Option<StoredMessage>>;
}

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

        Box::new(expect_ok(&client, req).and_then(move |_| {
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

            info!("sending request: {:?}", req);

            expect_ok(&client, req)
        }))
    }
}

fn http_err<A: DeserializeOwned + Send + 'static>(
    status: &StatusCode,
    body: &Chunk,
) -> Result<A, Error> {
    let body = String::from_utf8_lossy(&body.to_vec()).to_string();
    Err(format_err!(
        "Elasticsearch responded with non successful status code: {:?}. Message: {}",
        status,
        body
    ))
}

fn expect_ok(
    client: &Client<HttpConnector, Body>,
    req: Request<Body>,
) -> Box<Future<Item = (), Error = Error> + Send> {
    handle_response(client, req, |status_code, s| match status_code {
        _ if status_code.is_success() => Ok(()),
        _ => http_err(&status_code, &s),
    })
}

fn is_ok(
    client: &Client<HttpConnector, Body>,
    req: Request<Body>,
) -> Box<Future<Item = bool, Error = Error> + Send> {
    handle_response(client, req, |status_code, _| Ok(status_code.is_success()))
}

fn expect_option<A: DeserializeOwned + Send + 'static>(
    client: &Client<HttpConnector, Body>,
    req: Request<Body>,
) -> Box<Future<Item = Option<A>, Error = Error> + Send> {
    handle_response(client, req, |status_code, s| match status_code {
        _ if status_code.is_success() => serde_json::from_slice(s)
            .map(|a| Some(a))
            .map_err(|e| e.into()),
        _ if status_code.as_u16() == 404 => Ok(None),
        _ => http_err(&status_code, &s),
    })
}

fn expect<A: DeserializeOwned + Send + 'static>(
    client: &Client<HttpConnector, Body>,
    req: Request<Body>,
) -> Box<Future<Item = A, Error = Error> + Send> {
    handle_response(client, req, |status_code, s| match status_code {
        _ if status_code.is_success() => serde_json::from_slice(s).map_err(|e| e.into()),
        _ => http_err(&status_code, &s),
    })
}

fn handle_response<A, F>(
    client: &Client<HttpConnector, Body>,
    req: Request<Body>,
    handle_body: F,
) -> Box<Future<Item = A, Error = Error> + Send>
where
    A: DeserializeOwned + Send + 'static,
    F: Fn(&StatusCode, &Chunk) -> Result<A, Error> + Send + Sync + 'static,
{
    Box::new(
        client
            .request(req)
            .and_then(|res| {
                let status = res.status();
                res.into_body()
                    .concat2()
                    .map(move |chunks| (status, chunks))
            }).map_err(|e| e.into())
            .and_then(move |(status, body)| handle_body(&status, &body)),
    )
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

        Box::new(is_ok(&client, req).and_then(|initalised| {
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
                    expect_ok(
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
            expect_option::<EsDoc<StoredMessage>>(&client, req)
                .map(|maybe_doc| maybe_doc.map(|doc| doc._source)),
        )
    }

    fn search(
        &self,
        query: MessageQuery,
    ) -> Box<Future<Item = Vec<StoredMessage>, Error = Error> + Send> {
        let client = Client::new();
        let search_url = self.config.search_url().unwrap();
        let json_query: Value = query.into();
        debug!(
            "sending ES query: {:?} to {:?}",
            serde_json::to_string_pretty(&json_query).unwrap(),
            search_url
        );

        let body = Body::wrap_stream(stream::once(serde_json::to_string(&json_query)));
        let req = Request::builder()
            .method(Method::POST)
            .uri(search_url.to_string())
            .body(body)
            .expect("couldn't build a request!");

        Box::new(expect::<EsResult<TimestampedMessage>>(&client, req).map(|es_res| es_res.into()))
    }
}
