use futures::sync::mpsc::Receiver;
use futures::{Future, Stream};
use futures::stream;
use rmq::TimestampedMessage;
use std::boxed::Box;
use failure::{Error};
use std::sync::Arc;
use url::{Url, ParseError};
use serde::de::DeserializeOwned;
use hyper::Client;
use hyper::client::HttpConnector;
use hyper::StatusCode;
use hyper::Request;
use hyper::Method;
use hyper::Body;
use hyper::Chunk;
use serde_json;

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
  fn mapping_url(&self) -> Result<Url, ParseError>;
}

impl EsEndpoints for Config {
    fn message_url(&self, id: Option<String>) -> Result<Url, ParseError> {
       let without_id = self.index_url().and_then(|u| u.join(&format!("{}/", self.doc_type)))?;

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


fn http_err<A: DeserializeOwned + Send + 'static>(status: &StatusCode, body: &Chunk) -> Result<A, Error> {
    let body = String::from_utf8_lossy(&body.to_vec()).to_string();
    Err(format_err!("Elasticsearch responded with non successful status code: {:?}. Message: {}", status, body))
}

fn expect_ok(client: &Client<HttpConnector, Body>, req: Request<Body>) -> Box<Future<Item=(), Error=Error> + Send> {
    handle_response(client, req, |status_code, s| {
        match status_code {
            _ if status_code.is_success() => Ok(()),
            _ => http_err(&status_code, &s),
        }
    })
}

fn expect_option<A: DeserializeOwned + Send + 'static>(client: &Client<HttpConnector, Body>, req: Request<Body>) -> Box<Future<Item=Option<A>, Error=Error> + Send> {
    handle_response(client, req, |status_code, s| {
        match status_code {
            _ if status_code.is_success() => serde_json::from_slice(s).map(|a| Some(a)).map_err(|e| e.into()),
            _ if status_code.as_u16() == 404 => Ok(None),
            _ => http_err(&status_code, &s),
        }
    })
}

fn _expect<A: DeserializeOwned + Send + 'static>(client: &Client<HttpConnector, Body>, req: Request<Body>) -> Box<Future<Item=A, Error=Error> + Send> {
    handle_response(client, req, |status_code, s| {
        match status_code {
            _ if status_code.is_success() => serde_json::from_slice(s).map_err(|e| e.into()),
            _ => http_err(&status_code, &s),
        }
    })
}

fn handle_response<A, F>(client: &Client<HttpConnector, Body>, req: Request<Body>, handle_body: F) -> Box<Future<Item=A, Error=Error> + Send>
  where
    A: DeserializeOwned + Send + 'static,
    F: Fn(&StatusCode, &Chunk) -> Result<A, Error> + Send + Sync + 'static {
    Box::new(client
        .request(req)
        .and_then(|res| {
            let status = res.status();
            res.into_body().concat2().map(move |chunks| (status, chunks))
        })
        .map_err(|e| e.into())
        .and_then(move |(status, body)| {
            handle_body(&status, &body)
        }))
}


impl MessageSearchService for MessageStore {
    // NOTE: what happens when the mappings change?
    // ES provides a convenient API for that: https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-reindex.html
    // perhaps this tool should automatically manage migrations by managing two indices at the same time...
    fn init_store(&self) -> Task {
        let client = Client::new();
        let index_url = self.config.index_url().unwrap();
        let mappings_url = self.config.mapping_url().unwrap();

        let req =
            Request::builder()
              .method(Method::POST)
              .uri(index_url.to_string())
              .body(Body::empty())
              .expect("couldn't build a request!");

        Box::new(expect_ok(&client, req).and_then(move |_|{
            let mappings: serde_json::Value = json!({
                "properties": {
                  "received_at": {
                    "type": "date",
                    "index": "not_analyzed"
                  },
                  "message": {
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

            let req =
                Request::builder()
                    .method(Method::PUT)
                    .uri(mappings_url.to_string())
                    .body(Body::wrap_stream(stream::once(serde_json::to_string(&mappings))))
                    .expect("couldn't build a request!");

            info!("sending request: {:?}", req);

            expect_ok(&client, req)
        }))
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
                expect_ok(&client, req.body(body).expect(&format!("couldn't build HTTP request {:?}", msg)))

            }).for_each(|_| Ok(())))
    }

    fn message_for(&self, id: String) -> IoFuture<Option<TimestampedMessage>> {
        let client = Client::new();
        let index_url = self.config.message_url(Some(id)).unwrap();
        let req =
            Request::builder()
              .method(Method::GET)
              .uri(index_url.to_string())
              .body(Body::empty())
              .expect("couldn't build a request!");

        Box::new(expect_option::<EsDoc<TimestampedMessage>>(&client, req).map(|maybe_doc| maybe_doc.map(|doc| doc._source)))

    }
}


