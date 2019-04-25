extern crate env_logger;

#[macro_use]
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
use rmqfwd::opt::{Command, ConfigFile};
use rmqfwd::rmq;
use rmqfwd::rmq::TimestampedMessage;
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::runtime::Runtime;

fn main() -> Result<(), Error> {
    env_logger::init();
    let cmd = Command::from_args();
    let config_file_path = cmd
        .config_file()
        .unwrap_or_else(|| PathBuf::from("/etc/rmqfwd.toml"));
    let err_msg = format!(
        "could not parse configuration from {}",
        &config_file_path.clone().to_string_lossy()
    );

    let config = read_config(config_file_path).map_err(|e| format_err!("{}: {}", err_msg, e))?;

    let rt = Runtime::new().unwrap();

    match cmd {
        Command::Trace { api_port, .. } => run_trace(config, rt, api_port),

        Command::Export {
            filters,
            target_dir,
            pretty_print,
            force,
            ..
        } => run_export(config, rt, filters, target_dir, pretty_print, force),
        Command::Republish {
            filters,
            target_exchange,
            target_routing_key,
            ..
        } => run_republish(config, rt, filters, target_exchange, target_routing_key),
    }
}

fn run_trace(config: Config, mut rt: Runtime, api_port: u16) -> Result<(), Error> {
    use hyper::service::service_fn;
    use hyper::Server;
    use rmqfwd::http;

    let (tx, rx) = mpsc::channel::<TimestampedMessage>(5);

    let msg_store = rt.block_on(MessageStore::detecting_es_version(config.elasticsearch))?;
    let msg_store = Arc::new(msg_store);
    let msg_store2 = msg_store.clone();

    let addr = ([127, 0, 0, 1], api_port).into();

    let new_service = move || {
        let msg_store = msg_store2.clone();
        service_fn(move |req| http::routes(&msg_store, req))
    };

    let server = Server::bind(&addr)
        .serve(new_service)
        .map_err(|e| eprintln!("server error: {}", e));

    let msg_store = msg_store.clone();

    rt.block_on(msg_store.init_store())?;

    rt.spawn(server);
    rt.spawn(msg_store.write(rx).map_err(|_| ()));

    rt.block_on(rmq::bind_and_consume(config.rabbitmq, tx))?;
    Ok(())
}

fn run_export(
    config: Config,
    mut rt: Runtime,
    filters: rmqfwd::opt::Filters,
    target_dir: PathBuf,
    pretty_print: bool,
    force: bool,
) -> Result<(), Error> {
    let exporter = Exporter::new(pretty_print, force);
    let query: MessageQuery = try_from::TryFrom::try_from(filters)
        .map_err(|e| format_err!("an error occurred: {}", e))?;
    rt.block_on(
        MessageStore::detecting_es_version(config.elasticsearch).and_then(|msg_store| {
            msg_store
                .search(query)
                .and_then(move |es_result| exporter.export_messages(es_result.into(), target_dir))
        }),
    )?;
    info!("export completed!");
    Ok(())
}

fn run_republish(
    config: Config,
    mut rt: Runtime,
    filters: rmqfwd::opt::Filters,
    target_exchange: String,
    target_routing_key: Option<String>,
) -> Result<(), Error> {
    let query: MessageQuery = try_from::TryFrom::try_from(filters)
        .map_err(|e| format_err!("an error occurred: {}", e))?;
    let config = config.clone();
    rt.block_on(Box::new(
        MessageStore::detecting_es_version(config.elasticsearch.clone()).and_then(|msg_store| {
            msg_store.search(query).and_then(|es_result| {
                let stored_msgs: Vec<StoredMessage> = es_result.into();
                rmq::publish(
                    config.rabbitmq,
                    target_exchange,
                    target_routing_key,
                    stored_msgs,
                )
            })
        }),
    ))?;

    info!("Done replaying!");

    Ok(())
}
