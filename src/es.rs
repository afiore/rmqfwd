use futures::sync::mpsc::Receiver;
use futures::{Future, Stream};
use futures::future;
use rmq::Message;
use std::boxed::Box;
use rs_es::Client;
use std::sync::{Arc, Mutex};
use std::error::Error;
use rs_es::operations::mapping::*;
use rs_es::query::Query;

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

pub type Task = Box<Future<Item=(), Error=()> + Send>;

pub trait MessageSearchService {
    fn write(&self, rx: Receiver<Message>) -> Task;
    fn init_index(&self) -> Task;
}

pub struct MessageSearch {
    es_client: Arc<Mutex<Client>>,
    config: Arc<Config>,
}

impl MessageSearch {
    pub fn new(config: Config) -> Self {
        let err_msg = format!("Unable to parse Elasticsearch URL {}", config.base_url);
        let es_client = Arc::new(Mutex::new(Client::new(&config.base_url).expect(&err_msg)));
        MessageSearch { es_client: es_client, config: Arc::new(config) }
    }

}

impl MessageSearchService for MessageSearch {
    // NOTE: what happens when the mappings change?
    // ES provides a convenient API for that: https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-reindex.html
    // perhaps this tool should automatically manage migrations by managing two indices at the same time...
    fn init_index(&self) -> Task {
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

    fn write(&self, rx: Receiver<Message>) -> Task {
        let es_mutex = self.es_client.clone();
        let config = self.config.clone();

        Box::new(rx.and_then(move |msg| {
           let mut es_client = es_mutex.lock().unwrap();
           let mut indexer = es_client.index(&config.index, &config.doc_type);

           indexer
             .with_doc(&msg)
             .send()
             .map(|_| debug!("writing message to Elasticsearch: {:?}", msg))
             .map_err(|err| error!("ES returned an error: {}", err.description()))
        }).collect().then(|_| Ok(())))
    }
}