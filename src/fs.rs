use es::StoredMessage;
use std::path::PathBuf;
use failure::Error;
use futures::{Future, future};
use tokio::fs::file::File;
use std::fs::{remove_dir_all, read_dir, create_dir};
use serde_json;


pub trait Export {
   fn export_messages(&self, messages: Vec<StoredMessage>, target: PathBuf) -> Box<Future<Item=(), Error=Error> + Send>;
}

#[derive(Debug)]
pub struct Exporter {
    pretty_print: bool,
    force: bool,
}

impl Exporter {
    pub fn new(pretty_print: bool, force: bool) -> Self {
        Exporter {
            pretty_print: pretty_print,
            force: force,
        }
    }
}

impl Export for Exporter {
    fn export_messages(&self, messages: Vec<StoredMessage>, target: PathBuf) -> Box<Future<Item=(), Error=Error> + Send> {
        let pretty_print = self.pretty_print;
        let target1 = target.clone();
        let target2 = target.clone();
        let force1 = self.force.clone();
        let force2 = self.force.clone();

        debug!("exporter: {:?}", self);

        let setup_target = future::lazy(move || {
            if !target.exists() {
                info!("creating directory {}", target1.display());
                create_dir(target1).map_err(|e| e.into())
            } else {
                read_dir(target).map_err(|e| e.into()).and_then(|mut entries| {
                    if let Some(_) = entries.next() {
                        if force1 {
                            warn!("removed target directory {:?}", target1.display());
                            remove_dir_all(target1).map_err(|e| e.into())
                        } else {
                            Err(format_err!("target directory {:?} already exists!", target1.display()))
                        }
                    } else { Ok(()) }
                })
            }

        });

        //TODO: can I just reuse `export_message`? compiler says no.. "cannot infer an appropriate lifetime due to conflicting requirements"
        let run_export = future::join_all(messages.into_iter().map(move |msg| {
            let target = target2.join(&format!("{}.json", msg.id));

            future::lazy(move || {
                if target.exists() && !force2 {
                    Err(format_err!("file {:?} already exists!", target.display()))
                } else {
                    Ok(target)
                }
            }).and_then(|target| {
                info!("exporting {:?}", target.display());
                File::create(target).from_err()
            }).and_then(move |file| {
                let result = if pretty_print {
                    serde_json::to_writer_pretty(file, &msg)
                } else {
                    serde_json::to_writer(file, &msg)
                };

                result.map_err(|e| e.into())
            })
        })).map(|_| ());

        Box::new(setup_target.and_then(|_| run_export))
    }
}
