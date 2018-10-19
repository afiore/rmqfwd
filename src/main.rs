extern crate env_logger;
extern crate futures;
extern crate rmqfwd;
extern crate tokio;
extern crate tokio_codec;
extern crate failure;

#[macro_use] extern crate log;
#[macro_use] extern crate clap;

use clap::{App, SubCommand};
use failure::Error;
use futures::prelude::*;
use futures::sync::mpsc;
use rmqfwd::es;
use rmqfwd::es::{MessageSearchService, MessageStore, MessageQuery, StoredMessage};
use rmqfwd::rmq;
use rmqfwd::rmq::{Config, TimestampedMessage};
use rmqfwd::fs::*;
use std::io::BufReader;
use std::io::Write;
use tokio::fs::file::File;
use tokio::io;
use std::path::PathBuf;
use tokio::runtime::Runtime;

fn main() {
    env_logger::init();

    pub (crate) mod arg {
        pub mod replay {
            use clap::Arg;

            pub fn ids_file() -> Arg<'static,'static> {
                Arg::with_name("ids-file")
                    .required(true)
                    .takes_value(true)
                    .short("f")
                    .long_help("path to a file listing message ids (one per line)")
            }


            pub fn routing_key() -> Arg<'static, 'static> {
                Arg::with_name("routing-key")
                    .required(false)
                    .takes_value(true)
                    .short("k")
                    .long_help("the message routing key")
            }


            pub fn exchange() -> Arg<'static, 'static> {
                Arg::with_name("exchange")
                    .required(true)
                    .takes_value(true)
                    .short("e")
                    .long_help("the exchange where the message will be published")
            }
        }

        pub mod export {
            use clap::Arg;
            pub fn exchange() -> Arg<'static, 'static> {
                Arg::with_name("exchange")
                    .required(true)
                    .takes_value(true)
                    .short("e")
                    .long_help("the exchange where the message is published")
            }

            pub fn msg_body() -> Arg<'static, 'static> {
                Arg::with_name("message-body")
                    .required(false)
                    .takes_value(true)
                    .short("b")
                    .long_help("a string keyword to be matched against the message body")
            }

            pub fn routing_key() -> Arg<'static, 'static> {
                Arg::with_name("routing-key")
                    .required(false)
                    .takes_value(true)
                    .short("k")
                    .long_help("the message routing key")
            }

            pub fn target() -> Arg<'static, 'static> {
                Arg::with_name("target")
                    .index(1)
                    .required(true)
                    .takes_value(true)
                    .long_help("the export target file")
            }

            pub fn pretty_print() -> Arg<'static, 'static> {
                Arg::with_name("pretty-print")
                    .short("p")
                    .required(false)
                    .takes_value(false)
                    .long_help("pretty print message")
            }

            pub fn force() -> Arg<'static, 'static> {
                Arg::with_name("force")
                    .short("f")
                    .required(false)
                    .takes_value(false)
                    .long_help("force file writes, even when files exist in the target directory")
            }
        }
    }


    let app = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!())
        .subcommand(
            SubCommand
            ::with_name("trace")
                .about("bind a queue to 'amq.rabbitmq.trace' and persists received messages into the message store "))
        .subcommand(
            SubCommand
            ::with_name("export")
                .about("query the message store and write the result to the file system")
                .args(&[arg::export::exchange(), arg::export::routing_key(), arg::export::msg_body(),
                        arg::export::target(), arg::export::pretty_print(), arg::export::force()])
        )
        .subcommand(
            SubCommand
            ::with_name("replay")
                .about("replay a set of message ids")
                .args(&[arg::replay::exchange(), arg::replay::ids_file(), arg::replay::routing_key()])
        );

    //TODO: check if no argument has been supplied at all, in that case just print long help

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
            let exchange = matches.value_of("exchange").unwrap().to_string();
            let routing_key = matches.value_of("routing-key").map(|s| s.to_string());
            let body = matches.value_of("body").map(|s| s.to_string());

            let target: PathBuf = matches.value_of_os("target").unwrap().clone().into();
            debug!("matches: {:?}",matches);
            let pretty_print = matches.occurrences_of("pretty-print") > 0;
            let force = matches.occurrences_of("force") > 0;

            let msg_store = MessageStore::new(es::Config::default());
            let exporter = Exporter::new(pretty_print, force);

            let mut rt = Runtime::new().unwrap();
            let query = MessageQuery {
                exchange : exchange,
                routing_key: routing_key,
                body: body,
                time_range: None
            };
            debug!("search query: {:?}", query);

            let result = rt.block_on(msg_store.search(query).and_then(move |docs| {
                exporter.export_messages(docs, target)
            }));
            match result {
                Ok(_) => info!("export completed."),
                Err(e) => {
                    error!("Failed to export: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Some("replay") => {
            let matches = matches.subcommand_matches("replay").unwrap();
            let msg_store = MessageStore::new(es::Config::default());
            let _routing_key = matches.value_of("routing-key");
            let _exchange = matches.value_of("exchange");
            let path = matches.value_of("ids-file").unwrap();

            let mut rt = Runtime::new().unwrap();

            let x = Box::new(File::open(path.to_string()).map_err(|e| e.into()))
                .and_then(|file| {
                    let reader = BufReader::new(file);
                    //TODO: avoid move here
                    io::lines(reader).map_err(|e| e.into()).and_then(move |doc_id| {
                        let id = doc_id.clone();
                        msg_store.message_for(doc_id).map(|maybe_doc| (id, maybe_doc))
                    }).collect()
                }); //TODO: sink into rabbit publisher...

            let result: Result<Vec<(String, Option<StoredMessage>)>, Error> = rt.block_on(x);

            match result {
                Err(err) => {
                   error!("Could not open file {}. error: {:?}", path, err);
                   std::process::exit(1);

                }
                Ok(docs) => {
                    for (doc_id, maybe_doc) in docs {
                        if let Some(doc) = maybe_doc {
                           println!("got a message: {:?}", doc);
                        } else {
                           eprintln!("couldn't find a stored message with id: {}", doc_id);
                        }
                    }
                }
            }
        }
        _ => {
            matches.usage.clone().and_then(|s| write!(&mut std::io::stderr(), "{}", s).ok())
                .expect("Cannot write help to standard error");
            std::process::exit(1);
        }
    }
}
