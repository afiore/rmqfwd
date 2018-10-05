extern crate env_logger;
extern crate futures;
extern crate rmqfwd;
extern crate tokio;
extern crate tokio_codec;

#[macro_use] extern crate log;
#[macro_use] extern crate failure;
#[macro_use] extern crate clap;

use clap::{App, Arg, SubCommand};
use failure::Error;
use futures::prelude::*;
use futures::sync::mpsc;
use rmqfwd::es;
use rmqfwd::es::{MessageSearchService, MessageStore};
use rmqfwd::rmq;
use rmqfwd::rmq::{Config, TimestampedMessage};
use rmqfwd::fs::*;
use std::io::BufReader;
use std::io::Write;
use tokio::fs::file::File;
use tokio::io;
use tokio::runtime::Runtime;

fn main() {
    env_logger::init();


    let arg_exchange =
        Arg::with_name("exchange")
            .required(true)
            .takes_value(true)
            .short("e")
            .long_help("name of the exchange in which to publish");

    let arg_ids_file =
        Arg::with_name("ids-file")
            .required(true)
            .takes_value(true)
            .short("f")
            .long_help("path to a file listing message ids (one per line)");

    let arg_routing_key =
        Arg::with_name("routing-key")
            .required(false)
            .takes_value(true)
            .short("k")
            .long_help("the routing key to use");

    let arg_msg_id =
        Arg::with_name("message-id")
            .index(1)
            .required(true)
            .takes_value(true)
            .long_help("the id of the message to export");

    let arg_export_target =
        Arg::with_name("target")
            .index(2)
            .required(true)
            .takes_value(true)
            .long_help("the export target file");

    let arg_pretty_print =
        Arg::with_name("pretty-print")
            .short("p")
            .required(false)
            .takes_value(false)
            .long_help("pretty print message");

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
                .about("fetch a message from the store and write it to the file system")
                .args(&[arg_msg_id, arg_export_target, arg_pretty_print])
        )
        .subcommand(
            SubCommand
            ::with_name("replay")
                .about("replay a set of message ids")
                .args(&[arg_exchange, arg_ids_file, arg_routing_key])
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
            let msg_id = matches.value_of("message-id").unwrap();
            let target = matches.value_of_os("target").unwrap().clone().into();
            let pretty_print = value_t!(matches, "pretty-print", bool).unwrap_or(false);

            let msg_store = MessageStore::new(es::Config::default());

            let mut rt = Runtime::new().unwrap();

            let export = Box::new(msg_store.message_for(msg_id.clone().to_string()).and_then(move |maybe_msg|
              maybe_msg.ok_or(format_err!("couldn't find a message with id"))
            ).and_then(move |msg| Exporter::new(pretty_print).export_message(msg, target)));

            let result = rt.block_on(export);
            match result {
                Ok(_) => (),
                Err(e) => {
                    error!("Couldn't export the message {}", e);
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

            let result: Result<Vec<(String, Option<TimestampedMessage>)>, Error> = rt.block_on(x);

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
