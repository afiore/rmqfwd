use futures::sync::mpsc::Receiver;
use futures::{Future, Stream};
use futures::future;
use rmq::Message;
use rs_es::Client;

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

pub fn write_messages(rx: Receiver<Message>, config: Config) -> impl Future<Item=(), Error=()> {
    rx.and_then(move |msg| {
        let base_url = config.base_url.clone();
        let err_msg = format!("Unable to parse Elasticsearch URL {}", config.base_url);
        let mut es_client = Client::new(&base_url).expect(&err_msg);
        let mut indexer = es_client.index(&config.index, &config.doc_type);

        match indexer.with_doc(&msg).send() {
            Result::Ok(_) =>
              future::ok(debug!("writing message to Elasticsearch: {:?}", &msg)),
            Result::Err(err) =>
              future::ok(error!("ES returned an error: {:?}", err))
        }

    }).collect()
        .then(|_| Ok(()))

}