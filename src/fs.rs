use rmq::TimestampedMessage;
use std::path::PathBuf;
use failure::Error;
use futures::Future;
use futures::future;
use tokio::fs::file::File;
use serde_json;


trait Export {
   fn export_message(msg: TimestampedMessage, target: PathBuf) -> Box<Future<Item=(), Error=Error> + Send>;
}

pub struct Exporter;

impl Export for Exporter {
    fn export_message(msg: TimestampedMessage, target: PathBuf) -> Box<Future<Item=(), Error=Error> + Send> {
        Box::new(future::lazy(|| {
            if target.exists() {
                Err(format_err!("file {:?} already exists!", target.display()))
            } else {
                Ok(target)
            }
        }).and_then(|target| {
           File::create(target).map_err(|e| e.into())
        }).and_then(move |file| {
            serde_json::to_writer(file, &msg).map_err(|e| e.into())

        }))
    }
}
