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
extern crate clap;

extern crate structopt;

use failure::Error;
use futures::prelude::*;
use futures::sync::mpsc;
use rmqfwd::es::query::MessageQuery;
use rmqfwd::es::{MessageSearchService, MessageStore, StoredMessage};
use rmqfwd::fs::*;
use rmqfwd::opt::Command;
use rmqfwd::rmq;
use rmqfwd::rmq::TimestampedMessage;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

fn main() {
    env_logger::init();
    use structopt::StructOpt;
    let cmd = Command::from_args();
    match cmd {
        Command::Trace {
            rmq_config,
            es_config,
            api_port,
        } => {
            use hyper::service::service_fn;
            use hyper::Server;
            use rmqfwd::http;

            let (tx, rx) = mpsc::channel::<TimestampedMessage>(5);
            let mut rt = Runtime::new().unwrap();

            //TODO: use only one arc/mutex
            let msg_store = rt
                .block_on(MessageStore::detecting_es_version(es_config))
                .expect("couldn't determine ES Version");
            let msg_store2 = Arc::new(Mutex::new(msg_store.clone()));

            let addr = ([127, 0, 0, 1], api_port).into();

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

            rt.block_on(rmq::bind_and_consume(rmq_config, tx))
                .expect("runtime error!");
        }
        Command::Export {
            es_config,
            filters,
            target_dir,
            pretty_print,
            force,
        } => {
            let exporter = Exporter::new(pretty_print, force);
            let result: Result<MessageQuery, Error> = try_from::TryFrom::try_from(filters);

            match result {
                Err(_) => {
                    //TODO: display value here
                    error!("Couldn't parse a time range from supplied --since/--until values. Valid input would look like: 2018-07-08T09:10:11.012Z");
                    std::process::exit(1);
                }
                Ok(query) => {
                    let mut rt = Runtime::new().unwrap();
                    let result = rt.block_on(
                        MessageStore::detecting_es_version(es_config).and_then(|msg_store| {
                            msg_store.search(query).and_then(move |es_result| {
                                exporter.export_messages(es_result.into(), target_dir)
                            })
                        }),
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
        Command::Republish {
            rmq_config,
            es_config,
            filters,
            target_exchange,
            target_routing_key,
        } => {
            let result: Result<MessageQuery, Error> = try_from::TryFrom::try_from(filters);

            match result {
                //TODO: match a specific error here
                Err(_) => {
                    error!("Couldn't parse a time range from supplied --since/--until values. Valid input would look like: 2018-07-08T09:10:11.012Z");
                    std::process::exit(1);
                }
                Ok(query) => {
                    let mut rt = Runtime::new().unwrap();

                    let result = rt.block_on(Box::new(
                        MessageStore::detecting_es_version(es_config).and_then(|msg_store| {
                            msg_store.search(query).and_then(|es_result| {
                                let stored_msgs: Vec<StoredMessage> = es_result.into();
                                rmq::publish(
                                    rmq_config,
                                    target_exchange,
                                    target_routing_key,
                                    stored_msgs,
                                )
                            })
                        }),
                    ));

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
    }
}
