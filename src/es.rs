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
    static ref MAPPINGS: Mapping<'static> = {
        let mut index_mappings: Mapping = HashMap::new();
        let mut message_mappings: DocType = HashMap::new();

        let mut exchange_prop = HashMap::new();
        exchange_prop.insert("type", "string");
        exchange_prop.insert("index", "not_analyzed");

        let mut routing_key_prop = HashMap::new();
        routing_key_prop.insert("type", "string");
        routing_key_prop.insert("index", "not_analyzed");

        message_mappings.insert("exchange", exchange_prop);
        message_mappings.insert("routing_key", routing_key_prop);
        index_mappings.insert("message", message_mappings);
        
        index_mappings
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
    fn init_index(&mut self) -> Result<(), Box<Error>> {
        let mut mapping_op = MappingOperation::new(&mut self.es_client, &self.config.index);
        mapping_op.with_mapping(&MAPPINGS).send().map(|_| ()).map_err(|err| Box::new(err) as Box<Error>)
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