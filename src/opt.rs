use rmq::{Direction, UserCreds};
use std::collections::HashMap;
use std::path::PathBuf;
use structopt::StructOpt;

//TODO: break into two config structs
#[derive(Debug, Clone, StructOpt, Deserialize)]
pub struct RmqConfig {
    /// Set the RabbitMQ host
    #[structopt(long = "rmq-host", default_value = "127.0.0.1")]
    pub host: String,

    /// Set the RabbitMQ port
    #[structopt(long = "rmq-port", default_value = "5672")]
    pub port: u16,

    /// Set the RabbitMQ tracing exchange
    #[structopt(long = "rmq-exchange", default_value = "amq.rabbitmq.trace")]
    pub tracing_exchange: String,

    /// Set the Rabbitmq access credentials
    #[structopt(long = "rmq-creds")]
    pub creds: Option<UserCreds>,
}

#[derive(Debug, Clone, StructOpt, Deserialize)]
pub struct EsConfig {
    /// Set the Elasticsearch index
    #[structopt(long = "es-index", default_value = "rabbit_messages")]
    pub index: String,

    /// Set the Elasticsearch type
    #[structopt(long = "es-type", default_value = "message")]
    pub message_type: String,
    //TODO: parse to Url type
    /// Set the Elasticsearch base url
    #[structopt(long = "es-base-url", default_value = "http://localhost:9200")]
    pub base_url: String,
}

#[derive(Debug, StructOpt)]
pub struct Filters {
    //TODO: parse?
    /// Include only messages published after the supplied datetime")
    #[structopt(long = "since", short = "s")]
    pub since: Option<String>,

    /// Include only messages published before the supplied datetime")
    #[structopt(long = "until", short = "u")]
    pub until: Option<String>,

    /// Filter by exchange name
    #[structopt(long = "exchange", short = "e")]
    pub exchange: Option<String>,

    /// Filter by message direction (i.e. either 'entering' or 'leaving' the cluster)
    #[structopt(long = "direction", short = "d")]
    pub direction: Option<Direction>,

    /// Filter by routing key
    #[structopt(long = "routing-key", short = "k")]
    pub routing_key: Option<String>,

    /// A string to be matched against the message body")
    #[structopt(long = "message-body", short = "b")]
    pub message_body: Option<String>,

    //TODO: id should conflict with any filtering clause
    /// Filter by one or multiple message ids
    //#[structopt(long = "id", raw(conflicts_with_all(&["since", "until", "exchange", "routing-key", "message-body"])))]
    #[structopt(long = "id")]
    pub id: Vec<String>,
}

impl Into<HashMap<String, Vec<String>>> for Filters {
    fn into(self) -> HashMap<String, Vec<String>> {
        let mut h = HashMap::new();

        if let Some(since) = self.since {
            h.insert("since".to_owned(), vec![since]);
        }
        if let Some(until) = self.until {
            h.insert("until".to_owned(), vec![until]);
        }
        if let Some(exchange) = self.exchange {
            h.insert("exchange".to_owned(), vec![exchange]);
        }
        if let Some(routing_key) = self.routing_key {
            h.insert("routing-key".to_owned(), vec![routing_key]);
        }
        if let Some(direction) = self.direction {
            h.insert("direction".to_owned(), vec![direction.into()]);
        }
        if let Some(message_body) = self.message_body {
            h.insert("message-body".to_owned(), vec![message_body]);
        }

        if !self.id.is_empty() {
            h.insert("id".to_owned(), self.id);
        }

        h
    }
}

#[derive(Debug, StructOpt)]
pub enum Command {
    /// Bind a queue to the tracing exchange (e.g. 'amq.rabbitmq.trace') and persists received messages into the message store
    #[structopt(name = "trace")]
    Trace {
        /// The configuration file path (will try to read /etc/rmqfwd.toml if not supplied)
        #[structopt(long = "config-file", short = "c")]
        config_file: Option<PathBuf>,
        #[structopt(long = "api-port", short = "p", default_value = "1337")]
        api_port: u16,
    },

    /// Republish a subset of the messages present in the data store to an arbitrary exchange"
    #[structopt(name = "republish")]
    Republish {
        /// The configuration file path (will try to read /etc/rmqfwd.toml if not supplied)
        #[structopt(long = "config-file", short = "c")]
        config_file: Option<PathBuf>,
        #[structopt(flatten)]
        filters: Filters,
        /// The exchange where the messages will be republished
        #[structopt(long = "target-exchange")]
        target_exchange: String,
        /// The routing key for the republished messages
        #[structopt(long = "target-routing-key")]
        target_routing_key: Option<String>,
    },
    /// Query the message store and write the result to the file system
    #[structopt(name = "export")]
    Export {
        /// The configuration file path (will try to read /etc/rmqfwd.toml if not supplied)
        #[structopt(long = "config-file", short = "c")]
        config_file: Option<PathBuf>,
        #[structopt(flatten)]
        filters: Filters,

        /// The export target dir
        #[structopt(parse(from_os_str))]
        target_dir: PathBuf,

        /// Pretty print JSON messages
        #[structopt(long = "pretty-print", short = "p")]
        pretty_print: bool,

        /// Delete files already existing in the export dir
        #[structopt(long = "force", short = "f")]
        force: bool,
    },
}

pub trait ConfigFile {
    fn config_file(&self) -> Option<PathBuf>;
}
impl ConfigFile for Command {
    fn config_file(&self) -> Option<PathBuf> {
        match self {
            Command::Trace { config_file, .. } => config_file.clone(),
            Command::Republish { config_file, .. } => config_file.clone(),
            Command::Export { config_file, .. } => config_file.clone(),
        }
    }
}
