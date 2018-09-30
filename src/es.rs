use futures::future;
use futures::sync::mpsc::Receiver;
use futures::{Future, Stream};
use futures::stream;
use rmq::TimestampedMessage;
use rs_es::operations::mapping::*;
use rs_es::query::Query;
use rs_es::Client;
use std::boxed::Box;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::io;
use url::{Url, ParseError};
use hyper::{Client as HttpClient};
use hyper::Request;
use hyper::Method;
use hyper::Body;
use serde_json;


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
    //TODO: this is blindly copy-pasted from rs_es tests. Undestand and review this settings!
    static ref SETTINGS: Settings = Settings {
            number_of_shards: 1,
            analysis: Analysis {
                filter: json! ({}).as_object().expect("by construction 'autocomplete_filter' should be a map").clone(),
                analyzer: json! ({}).as_object().expect("by construction 'autocomplete' should be a map").clone()
            }
        };
}

lazy_static! {
    static ref MAPPINGS: Mapping<'static> = {
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

pub type Task = Box<Future<Item = (), Error = ()> + Send>;
pub type IoFuture<A> = Box<Future<Item = A, Error = io::Error> + Send>;

//TODO: reimplement using plain Hyper client
pub trait MessageSearchService {
    fn write(&self, rx: Receiver<TimestampedMessage>) -> Task;
    fn init_store(&self) -> Task;
    fn message_for(&self, id: String) -> IoFuture<Option<TimestampedMessage>>;
}

pub struct MessageStore {
    es_client: Arc<Mutex<Client>>,
    config: Arc<Config>,
}

impl MessageStore {
    pub fn new(config: Config) -> Self {
        let err_msg = format!("Unable to parse Elasticsearch URL {}", config.base_url);
        let es_client = Arc::new(Mutex::new(Client::new(&config.base_url).expect(&err_msg)));
        MessageStore {
            es_client: es_client,
            config: Arc::new(config),
        }
    }
}

impl MessageSearchService for MessageStore {
    // NOTE: what happens when the mappings change?
    // ES provides a convenient API for that: https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-reindex.html
    // perhaps this tool should automatically manage migrations by managing two indices at the same time...
    fn init_store(&self) -> Task {
        let mutex = self.es_client.clone();
        let config = self.config.clone();

        Box::new(future::lazy(move || {
            //TODO: handle poisoning?
            let mut es_client = mutex.lock().unwrap();
            let result = es_client
                .count_query()
                .with_indexes(&[&config.index])
                .with_query(&Query::build_match_all().build())
                .send()
                .map(|_| ())
                .map_err(|_| ());

            if result.is_ok() {
                info!("index {} already exists", &config.index);
                Ok(())
            } else {
                let mut mapping_op = MappingOperation::new(&mut es_client, &config.index);
                mapping_op
                    .with_settings(&SETTINGS)
                    .with_mapping(&MAPPINGS)
                    .send()
                    .map(|_| ())
                    .map_err(|err| error!("Couldn't initialise index: {:?}", err))
            }
        }))
    }

    fn write(&self, rx: Receiver<TimestampedMessage>) -> Task {
        //TODO: handle error
        let ep_url = self.config.message_url(None).unwrap().to_string();

        Box::new(
            rx.and_then(move |msg| {
                let client = HttpClient::new();
                let body = Body::wrap_stream(stream::once(serde_json::to_string(&msg)));
                let mut req = Request::builder();

                req.method(Method::POST).uri(ep_url.clone());

                client
                    .request(req.body(body).expect(&format!("couldn't build HTTP request {:?}", msg)))
                    .map_err(|err| error!("ES connection error: {}", err.description()))
                    .and_then(|res| {
                        if res.status().is_success() {
                            Ok(())
                        } else {
                            error!("Elasticsearch responded with non successful status code: {:?}", res);
                            Err(())
                        }
                    })

            }).for_each(|_| Ok(())))
    }

    fn message_for(&self, id: String) -> IoFuture<Option<TimestampedMessage>> {
        let client = HttpClient::new();
        let url = self.config.message_url(Some(id)).unwrap();
        let req =
            Request::builder()
              .method(Method::GET)
              .uri(url.to_string())
              .body(Body::empty())
              .expect("couldn't build a request!");

        debug!("sending request: {:?}", req);

        Box::new(client.request(req).and_then(|res| {
          debug!("got a response: {:?}", res);

          let is_success = res.status().is_success();
          res.into_body().concat2().map(move |x| (is_success, x))
        }).and_then(|(is_success, body)| {
//            let v = body.to_vec();
//            let s = String::from_utf8_lossy(&v).to_string();
//            println!("got a body {}", s);

            if is_success {
                let doc: EsDoc<TimestampedMessage> = serde_json::from_slice(&body).unwrap();
                Ok(Some(doc._source))
            } else {
                Ok(None)
            }
           //TODO: use a more sane way to handle errors!
        }).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e)))
    }
}


