use futures::sync::mpsc::Receiver;
use futures::{Future, Stream};
use futures::future;
use futures::*;
use rmq::Message;
use std::boxed::Box;
use rs_es::Client;
use std::result::*;
use std::collections::HashMap;
use std::error::Error;
use rs_es::operations::mapping::*;
use serde_json::Value;

pub struct Config {
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

lazy_static! {
    //TODO: this is blindly copy-pasted from rs_es tests. Undestand and review this settings!
    static ref SETTINGS: Settings = Settings {
            number_of_shards: 1,
            analysis: Analysis {
                filter: json! ({
                    "autocomplete_filter": {
                        "type": "edge_ngram",
                        "min_gram": 1,
                        "max_gram": 2,
                    }
                }).as_object().expect("by construction 'autocomplete_filter' should be a map").clone(),
                analyzer: json! ({
                    "autocomplete": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [ "lowercase", "autocomplete_filter"]
                    }
                }).as_object().expect("by construction 'autocomplete' should be a map").clone()
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

              },
          }
    };
}

pub trait MessageSearchService {
    fn write(mut self, rx: Receiver<Message>) -> Box<Future<Item=(), Error=()> + Send>;
    fn init_index(&mut self) -> Result<(), Box<Error>>;
}

pub struct MessageSearch {
    es_client: Client,
    config: Config,
}

impl MessageSearch {
    pub fn new(config: Config) -> Self {
        let err_msg = format!("Unable to parse Elasticsearch URL {}", config.base_url);
        let es_client = Client::new(&config.base_url).expect(&err_msg);
        MessageSearch { es_client: es_client, config: config }
    }

}

impl MessageSearchService for MessageSearch {
    // NOTE: what happens when the mappings change?
    // ES provides a convenient API for that: https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-reindex.html
    // perhaps this tool should automatically manage migrations by managing two indices at the same time...
    fn init_index(&mut self) -> Result<(), Box<Error>> {
        let mut mapping_op = MappingOperation::new(&mut self.es_client, &self.config.index);
        mapping_op
          .with_settings(&SETTINGS)
          .with_mapping(&MAPPINGS)
          .send().map(|_| ()).map_err(|err| Box::new(err) as Box<Error>)
    }

    fn write(mut self, rx: Receiver<Message>) -> Box<Future<Item=(), Error=()> + Send> {
        Box::new(rx.and_then(move |msg| {
            let mut indexer = self.es_client.index(&self.config.index, &self.config.doc_type);

            match indexer.with_doc(&msg).send() {
                Result::Ok(_) =>
                  future::ok(debug!("writing message to Elasticsearch: {:?}", &msg)),
                Result::Err(err) =>
                  future::ok(error!("ES returned an error: {:?}", err))
            }

        }).collect()
            .then(|_| Ok(())))

    }
}