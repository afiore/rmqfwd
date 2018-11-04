extern crate env_logger;
extern crate failure;
extern crate futures;
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
use rmqfwd::es::{MessageSearchService, MessageStore};
use rmqfwd::fs::*;
use rmqfwd::rmq;
use rmqfwd::rmq::{Config, TimestampedMessage};
use std::io::Write;
use std::path::PathBuf;
use tokio::runtime::Runtime;

fn main() {
    env_logger::init();

    pub(crate) mod arg {
        pub mod common {
            use clap::Arg;
            pub fn since() -> Arg<'static, 'static> {
                Arg::with_name("since")
                    .required(false)
                    .takes_value(true)
                    .short("s")
                    .long_help("Include only messages published since the supplied datetime")
            }

            pub fn until() -> Arg<'static, 'static> {
                Arg::with_name("until")
                    .required(false)
                    .takes_value(true)
                    .short("u")
                    .long_help("Include only messages published before the supplied datetime")
            }

        }
        pub mod replay {
            use clap::Arg;

            pub fn exchange() -> Arg<'static, 'static> {
                Arg::with_name("exchange")
                    .required(true)
                    .takes_value(true)
                    .short("e")
                    .long_help("Filter by exchange name")
            }

            pub fn msg_body() -> Arg<'static, 'static> {
                Arg::with_name("message-body")
                    .required(false)
                    .takes_value(true)
                    .short("b")
                    .long_help("A string keyword to be matched against the message body")
            }

            pub fn routing_key() -> Arg<'static, 'static> {
                Arg::with_name("routing-key")
                    .required(false)
                    .takes_value(true)
                    .short("k")
                    .long_help("Filter by routing key")
            }

            pub fn target_routing_key() -> Arg<'static, 'static> {
                Arg::with_name("target-routing-key")
                    .required(false)
                    .takes_value(true)
                    .long_help("The routing key to use when replying the messages")
            }

            pub fn target_exchange() -> Arg<'static, 'static> {
                Arg::with_name("target-exchange")
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
                    .long_help("The exchange where the message is published")
            }

            pub fn msg_body() -> Arg<'static, 'static> {
                Arg::with_name("message-body")
                    .required(false)
                    .takes_value(true)
                    .short("b")
                    .long_help("A string keyword to be matched against the message body")
            }

            pub fn routing_key() -> Arg<'static, 'static> {
                Arg::with_name("routing-key")
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
                    .short("p")
                    .required(false)
                    .takes_value(false)
                    .long_help("Pretty-print message")
            }

            pub fn force() -> Arg<'static, 'static> {
                Arg::with_name("force")
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
                .about("Bind a queue to 'amq.rabbitmq.trace' and persists received messages into the message store "))
        .subcommand(
            SubCommand
            ::with_name("export")
                .about("Query the message store and write the result to the file system")
                .args(&[arg::common::since(), arg::common::until(),
                        arg::export::exchange(), arg::export::routing_key(), arg::export::msg_body(),
                        arg::export::target(), arg::export::pretty_print(), arg::export::force()])
        )
        .subcommand(
            SubCommand
            ::with_name("replay")
                .about("Publish a subset of the messages in the data store")
                .args(&[arg::common::since(), arg::common::until(),
                        arg::replay::exchange(), arg::replay::routing_key(), arg::replay::msg_body(),
                        arg::replay::target_exchange(), arg::replay::target_routing_key()])

        );

    let matches = app.get_matches();

    match matches.subcommand_name() {
        Some("trace") => {
            let (tx, rx) = mpsc::channel::<TimestampedMessage>(5);
            let msg_store = MessageStore::new(es::Config::default());

            let mut rt = Runtime::new().unwrap();

            rt.block_on(msg_store.init_store())
                .expect("MessageStore.init_index() failed!");

            rt.spawn(msg_store.write(rx).map_err(|_| ()));

            rt.block_on(rmq::bind_and_consume(Config::default(), tx))
                .expect("runtime error!");
        }
        Some("export") => {
            let matches = matches.subcommand_matches("export").unwrap();

            let target: PathBuf = matches.value_of_os("target").unwrap().clone().into();
            let pretty_print = matches.occurrences_of("pretty-print") > 0;
            let force = matches.occurrences_of("force") > 0;

            let msg_store = MessageStore::new(es::Config::default());
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
                    let result = rt.block_on(
                        msg_store
                            .search(query)
                            .and_then(move |docs| exporter.export_messages(docs, target)),
                    );
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
                    let msg_store = MessageStore::new(es::Config::default());

                    let result =
                        rt.block_on(Box::new(msg_store.search(query).and_then(|stored_msgs| {
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
