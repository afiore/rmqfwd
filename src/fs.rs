use rmq::TimestampedMessage;
use std::path::PathBuf;
use failure::Error;
use futures::Future;
use futures::future;
use tokio::fs::file::File;
use serde_json;


pub trait Export {
   fn export_message(&self, msg: TimestampedMessage, target: PathBuf) -> Box<Future<Item=(), Error=Error> + Send>;
}

pub struct Exporter {
    pretty_print: bool
}

impl Exporter {
    pub fn new(pretty_print: bool) -> Self {
        Exporter {
            pretty_print: pretty_print
        }
    }
}

impl Export for Exporter {
    fn export_message(&self, msg: TimestampedMessage, target: PathBuf) -> Box<Future<Item=(), Error=Error> + Send> {
        let pretty_print = self.pretty_print.clone();
        Box::new(future::lazy(|| {
            if target.exists() {
                Err(format_err!("file {:?} already exists!", target.display()))
            } else {
                Ok(target)
            }
        }).and_then(|target| {
           File::create(target).map_err(|e| e.into())
        }).and_then(move |file| {
            let result = if pretty_print {
                serde_json::to_writer_pretty(file, &msg)
            } else {
                serde_json::to_writer(file, &msg)
            };

            result.map_err(|e| e.into())

        }))
    }
}
