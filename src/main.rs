extern crate env_logger;
extern crate failure;
extern crate futures;
extern crate hyper;
extern crate rmqfwd;
extern crate tokio;
extern crate tokio_codec;
extern crate try_from;

#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;

use clap::{App, SubCommand};
use failure::Error;
use futures::prelude::*;
use futures::sync::mpsc;
use rmqfwd::es;
use rmqfwd::es::MessageQuery;
use rmqfwd::es::{MessageSearchService, MessageStore, StoredMessage};
use rmqfwd::fs::*;
use rmqfwd::rmq;
use rmqfwd::rmq::{Config, TimestampedMessage};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

fn main() {
    env_logger::init();

    pub(crate) mod arg {
        pub mod common {
            use clap::Arg;

            pub fn es_index() -> Arg<'static, 'static> {
                Arg::with_name("es-index")
                    .long("es-index")
                    .required(false)
                    .takes_value(true)
                    .long_help(
                        "Override the Elasticsearch index name (default value: 'rabbit_messages')",
                    )
            }

            pub fn es_type() -> Arg<'static, 'static> {
                Arg::with_name("es-type")
                    .long("es-type")
                    .required(false)
                    .takes_value(true)
                    .long_help("Override Elasticsearch type (default value: 'message')")
            }

            pub fn es_base_url() -> Arg<'static, 'static> {
                Arg::with_name("es-base-url")
                    .long("es-base-url")
                    .required(false)
                    .takes_value(true)
                    .long_help("Override Elasticsearch host (default value: 'http://localhost:9200')")
            }

            pub fn es_major_version() -> Arg<'static, 'static> {
                Arg::with_name("es-major-version")
                    .long("es-base-url")
                    .required(false)
                    .takes_value(true)
                    .long_help("Explicitly the expected Elastic Search API version major number (default value: autodetected)")
            }


            pub fn since() -> Arg<'static, 'static> {
                Arg::with_name("since")
                    .long("since")
                    .required(false)
                    .takes_value(true)
                    .short("s")
                    .long_help("Include only messages published since the supplied datetime")
            }

            pub fn until() -> Arg<'static, 'static> {
                Arg::with_name("until")
                    .long("until")
                    .required(false)
                    .takes_value(true)
                    .short("u")
                    .long_help("Include only messages published before the supplied datetime")
            }

            pub fn with(args: Vec<Arg<'static, 'static>>) -> Vec<Arg<'static, 'static>> {
                let mut common = vec![es_index(), es_type(), es_base_url(), es_major_version()];
                common.extend(args);
                common
            }
        }
        pub mod trace {
            use clap::Arg;

            pub fn port() -> Arg<'static, 'static> {
                Arg::with_name("api-port")
                    .required(false)
                    .takes_value(true)
                    .short("p")
                    .long_help("search API port (defaults to 1337)")
            }

        }
        pub mod replay {
            use clap::Arg;

            pub fn exchange() -> Arg<'static, 'static> {
                Arg::with_name("exchange")
                    .required(true)
                    .takes_value(true)
                    .short("e")
                    .long("exchange")
                    .long_help("Filter by exchange name")
            }

            pub fn msg_body() -> Arg<'static, 'static> {
                Arg::with_name("message-body")
                    .required(false)
                    .takes_value(true)
                    .short("b")
                    .long("message-body")
                    .long_help("A string keyword to be matched against the message body")
            }

            pub fn routing_key() -> Arg<'static, 'static> {
                Arg::with_name("routing-key")
                    .required(false)
                    .takes_value(true)
                    .short("k")
                    .long("routing-key")
                    .long_help("Filter by routing key")
            }

            pub fn target_routing_key() -> Arg<'static, 'static> {
                Arg::with_name("target-routing-key")
                    .long("target-routing-key")
                    .required(false)
                    .takes_value(true)
                    .long_help("The routing key to use when replying the messages")
            }

            pub fn target_exchange() -> Arg<'static, 'static> {
                Arg::with_name("target-exchange")
                    .long("target-exchange")
                    .required(true)
                    .takes_value(true)
                    .long_help("The exchange where the message will be published")
            }
        }

        pub mod export {
            use clap::Arg;
            pub fn exchange() -> Arg<'static, 'static> {
                Arg::with_name("exchange")
                    .required(true)
                    .takes_value(true)
                    .short("e")
                    .long("exchange")
                    .long_help("The exchange where the message is published")
            }

            pub fn msg_body() -> Arg<'static, 'static> {
                Arg::with_name("message-body")
                    .long("message-body")
                    .required(false)
                    .takes_value(true)
                    .short("b")
                    .long_help("A string keyword to be matched against the message body")
            }

            pub fn routing_key() -> Arg<'static, 'static> {
                Arg::with_name("routing-key")
                    .long("routing-key")
                    .required(false)
                    .takes_value(true)
                    .short("k")
                    .long_help("The message routing key")
            }

            pub fn target() -> Arg<'static, 'static> {
                Arg::with_name("target")
                    .index(1)
                    .required(true)
                    .takes_value(true)
                    .long_help("The export target file")
            }

            pub fn pretty_print() -> Arg<'static, 'static> {
                Arg::with_name("pretty-print")
                    .long("pretty-print")
                    .short("p")
                    .required(false)
                    .takes_value(false)
                    .long_help("Pretty-print message")
            }

            pub fn force() -> Arg<'static, 'static> {
                Arg::with_name("force")
                    .long("force")
                    .short("f")
                    .required(false)
                    .takes_value(false)
                    .long_help("Force file writes, even when files exist in the target directory")
            }
        }
    }

    let app = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!())
        .subcommand(
            SubCommand
            ::with_name("trace")
                .about("Bind a queue to 'amq.rabbitmq.trace' and persists received messages into the message store ")
                 .args(arg::common::with(vec![arg::trace::port()]).as_slice()))
            .subcommand(
                SubCommand
                ::with_name("export")
                    .about("Query the message store and write the result to the file system")
                    .args(arg::common::with(vec![
                                            arg::common::since(), arg::common::until(),
                                            arg::export::exchange(), arg::export::routing_key(), arg::export::msg_body(),
                                            arg::export::target(), arg::export::pretty_print(), arg::export::force()]).as_slice() ))
    
            .subcommand(
                SubCommand
                ::with_name("replay") //TODO: rename to republish
                    .about("Reublish a subset of the messages present in the data store to an arbitrary exchange")
                    .args(arg::common::with(vec![
                                            arg::common::since(), arg::common::until(),
                                            arg::replay::exchange(), arg::replay::routing_key(), arg::replay::msg_body(),
                                            arg::replay::target_exchange(), arg::replay::target_routing_key()]).as_slice()));
    
    let matches = app.get_matches();

    match matches.subcommand_name() {
        Some("trace") => {
            use hyper::service::service_fn;
            use hyper::Server;
            use rmqfwd::http;

            let matches = matches.subcommand_matches("trace").unwrap();
            let (tx, rx) = mpsc::channel::<TimestampedMessage>(5);
            //TODO: use only one arc/mutex
            let msg_store = MessageStore::new(es::Config::from(matches));
            let msg_store2 = Arc::new(Mutex::new(msg_store.clone()));

            let port = value_t!(matches, "api-port", u16).unwrap_or(1337);
            let mut rt = Runtime::new().unwrap();

            //TODO read value from es-base url param
            let addr = ([127, 0, 0, 1], port).into();

            let new_service = move || {
                let msg_store = msg_store2.clone();
                service_fn(move |req| http::routes(&msg_store, req))
            };

            let server = Server::bind(&addr)
                .serve(new_service)
                .map_err(|e| eprintln!("server error: {}", e));

            rt.block_on(msg_store.init_store())
                .expect("MessageStore.init_index() failed!");

            rt.spawn(server);
            rt.spawn(msg_store.write(rx).map_err(|_| ()));

            rt.block_on(rmq::bind_and_consume(Config::default(), tx))
                .expect("runtime error!");
        }
        Some("export") => {
            let matches = matches.subcommand_matches("export").unwrap();

            let target: PathBuf = matches.value_of_os("target").unwrap().clone().into();
            let pretty_print = matches.occurrences_of("pretty-print") > 0;
            let force = matches.occurrences_of("force") > 0;

            let msg_store = MessageStore::new(es::Config::from(matches));
            let exporter = Exporter::new(pretty_print, force);
            let result: Result<MessageQuery, Error> = try_from::TryFrom::try_from(matches);

            match result {
                Err(_) => {
                    //TODO: display value here
                    error!("Couldn't parse a time range from supplied --since/--until values");
                    std::process::exit(1);
                }
                Ok(query) => {
                    let mut rt = Runtime::new().unwrap();
                    let result = rt.block_on(msg_store.search(query).and_then(move |es_result| {
                        exporter.export_messages(es_result.into(), target)
                    }));
                    match result {
                        Ok(_) => info!("export completed."),
                        Err(e) => {
                            error!("Failed to export: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
            }
        }

        Some("replay") => {
            let matches = matches.subcommand_matches("replay").unwrap();

            let target_exchange = matches
                .value_of("target-exchange")
                .expect("expected 'target-exchange' argument")
                .to_string();
            let target_routing_key = matches
                .value_of("target-routing-key")
                .map(|s| s.to_string());

            let result: Result<MessageQuery, Error> = try_from::TryFrom::try_from(matches);

            match result {
                Err(_) => {
                    //TODO: display value here
                    error!("Couldn't parse a time range from supplied --since/--until values");
                    std::process::exit(1);
                }
                Ok(query) => {
                    let mut rt = Runtime::new().unwrap();
                    let msg_store = MessageStore::new(es::Config::from(matches));

                    let result =
                        rt.block_on(Box::new(msg_store.search(query).and_then(|es_result| {
                            let stored_msgs: Vec<StoredMessage> = es_result.into();
                            rmq::publish(
                                rmq::Config::default(),
                                target_exchange,
                                target_routing_key,
                                stored_msgs,
                            )
                        })));

                    match result {
                        Err(err) => {
                            error!("Couldn't replay messages. Error: {:?}", err);
                            std::process::exit(1);
                        }
                        Ok(_) => info!("done."),
                    }
                }
            }
        }

        _ => {
            matches
                .usage
                .clone()
                .and_then(|s| write!(&mut std::io::stderr(), "{}", s).ok())
                .expect("Cannot write help to standard error");
            std::process::exit(1);
        }
    }
}
