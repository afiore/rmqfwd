extern crate clap;
extern crate env_logger;
extern crate futures;
#[macro_use]
extern crate log;
extern crate rmqfwd;
extern crate tokio;
extern crate tokio_codec;
extern crate uuid;

use clap::{App, Arg, SubCommand};
use futures::future;
use futures::prelude::*;
use futures::stream;
use futures::sync::mpsc;
use rmqfwd::es;
use rmqfwd::es::{MessageSearchService, MessageStore};
use rmqfwd::rmq;
use rmqfwd::rmq::{Config, Message};
use std::io::BufReader;
use std::io::Write;
use tokio::fs::file::File;
//TODO: use tokio::fs
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


    let app = App::new("rmqfwd")
        .version("0.1")
        .author("Andrea Fiore")
        .subcommand(
            SubCommand
            ::with_name("trace")
                .about("bind a queue to 'amq.rabbitmq.trace' and persists received messages into the message store "))
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
            let (tx, rx) = mpsc::channel::<Message>(5);
            let msg_store = MessageStore::new(es::Config::default());

            let mut rt = Runtime::new().unwrap();
            rt.block_on(msg_store.init_store())
                .expect("MessageStore.init_index() failed!");
            rt.spawn(msg_store.write(rx));
            rt.block_on(rmq::bind_and_consume(Config::default(), tx))
                .expect("runtime error!");
        }
        Some("replay") => {
            let matches = matches.subcommand_matches("replay").unwrap();
            let msg_store = MessageStore::new(es::Config::default());

            let _routing_key = matches.value_of("routing-key");
            let _exchange = matches.value_of("exchange");
            let path = matches.value_of("ids-file").unwrap();

            let mut rt = Runtime::new().unwrap();

            let x = Box::new(File::open(path.to_string()))
                .and_then(|file| {
                    let reader = BufReader::new(file);
                    //TODO: use Stream.forward(sink)
                    io::lines(reader).and_then(move |doc_id| {
                        msg_store.message_for(doc_id)
                    }).collect()
                }); //TODO: sync to rabbit publisher...

            let result: Result<Vec<Message>, io::Error> = rt.block_on(x);

            match result {
                Err(err) => {
                   error!("Could not open file {}. error: {:?}", path, err);
                   std::process::exit(1);

                }
                Ok(docs) => {
                    for doc in docs {
                        println!("got a message: {:?}", doc);
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
