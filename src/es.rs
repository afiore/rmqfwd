use futures::sync::mpsc::Receiver;
use futures::{Future, Stream};
use futures::stream;
use rmq::TimestampedMessage;
use std::boxed::Box;
use failure::{Error};
use std::sync::Arc;
use std::collections::HashMap;
use url::{Url, ParseError};
use serde::de::DeserializeOwned;
use hyper::Client;
use hyper::client::HttpConnector;
use hyper::Request;
use hyper::Method;
use hyper::Body;
use serde_json;

pub type Mappings<'s> = HashMap<&'s str, HashMap<&'s str, HashMap<&'s str, &'s str>>>;
pub type Task = Box<Future<Item = (), Error = Error> + Send>;
pub type IoFuture<A> = Box<Future<Item = A, Error = Error> + Send>;

#[derive(Debug)]
pub struct Config {
    //TODO: add support for TLS
    base_url: String,
    index: String,
    doc_type: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EsDoc<A> {
    pub _source: A
}

trait EsEndpoints {
  fn message_url(&self, id: Option<String>) -> Result<Url, ParseError>;
  fn index_url(&self) -> Result<Url, ParseError>;
}

impl EsEndpoints for Config {
    fn message_url(&self, id: Option<String>) -> Result<Url, ParseError> {
       let without_id = self.index_url().and_then(|u| u.join(&format!("{}/", self.doc_type)))?;

       match id {
          Some(id) =>
              without_id.join(&format!("{}/", id)),
           _ => Ok(without_id),
       }
    }

    fn index_url(&self) -> Result<Url, ParseError> {
        Url::parse(&self.base_url).and_then(|u| u.join(&format!("{}/", &self.index)))
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

lazy_static! {
    static ref MAPPINGS: Mappings<'static> = {
        hashmap! {
            "message" => hashmap! {
                "exchange" => hashmap! {
                    "type" => "string",
                    "index" => "not_analyzed",
                },
                "routing_key" => hashmap! {
                    "type" => "string",
                    "index" => "not_analyzed",
                },
                "received_at" => hashmap! {
                    "type" => "date",
                    "index" => "not_analyzed",
                },
                "redelivered" => hashmap! {
                    "type" => "boolean",
                    "index" => "not_analyzed",
                },
                "routing_key" => hashmap! {
                    "type" => "string",
                    "index" => "not_analyzed",
                },
                "uuid" => hashmap! {
                    "type" => "string",
                    "index" => "not_analyzed",
                },
                "headers" => hashmap! {
                  "type" => "object",
                  "enabled" => "false"
                }
            },
        }
    };
}


//TODO: reimplement using plain Hyper client
pub trait MessageSearchService {
    fn write(&self, rx: Receiver<TimestampedMessage>) -> Task;
    fn init_store(&self) -> Task;
    fn message_for(&self, id: String) -> IoFuture<Option<TimestampedMessage>>;
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
}

fn expect<A>(client: &Client<HttpConnector, Body>, req: Request<Body>) -> Box<Future<Item=A, Error=Error> + Send>
  where
    A: DeserializeOwned + 'static + Send {
    Box::new(client
        .request(req)
        .and_then(|res| {
            let status = res.status();
            res.into_body().concat2().map(move |chunks| (status, chunks))
        })
        .map_err(|e| e.into())
        .and_then(|(status, body)| {
            if status.is_success() {
                let users = serde_json::from_slice(&body)?;
                Ok(users)
            } else {
                let s = String::from_utf8_lossy(&body.to_vec()).to_string();
                Err(format_err!("Elasticsearch responded with non successful status code: {:?}. Message: {}", status, s))
            }
        }))
}


impl MessageSearchService for MessageStore {
    // NOTE: what happens when the mappings change?
    // ES provides a convenient API for that: https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-reindex.html
    // perhaps this tool should automatically manage migrations by managing two indices at the same time...
    fn init_store(&self) -> Task {
        let _config = self.config.clone();
        unimplemented!()
    }


    fn write(&self, rx: Receiver<TimestampedMessage>) -> Task {
        //TODO: handle error
        let ep_url = self.config.message_url(None).unwrap().to_string();

        Box::new(
            rx.map_err(|_| format_err!("failed to receive message")).and_then(move |msg| {
                let client = Client::new();
                let body = Body::wrap_stream(stream::once(serde_json::to_string(&msg)));
                let mut req = Request::builder();

                req.method(Method::POST).uri(ep_url.clone());
                expect::<()>(&client, req.body(body).expect(&format!("couldn't build HTTP request {:?}", msg)))

            }).for_each(|_| Ok(())))
    }

    fn message_for(&self, id: String) -> IoFuture<Option<TimestampedMessage>> {
        let client = Client::new();
        let url = self.config.message_url(Some(id)).unwrap();
        let req =
            Request::builder()
              .method(Method::GET)
              .uri(url.to_string())
              .body(Body::empty())
              .expect("couldn't build a request!");

        expect(&client, req)

    }
}


