use es::StoredMessage;
use failure::Error;
use futures::{future, Future};
use serde_json;
use std::fs::{create_dir, read_dir, remove_dir_all};
use std::path::PathBuf;
use tokio::fs::file::File;

pub trait Export {
    fn export_messages(
        &self,
        messages: Vec<StoredMessage>,
        target: PathBuf,
    ) -> Box<Future<Item = (), Error = Error> + Send>;
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
    fn export_messages(
        &self,
        messages: Vec<StoredMessage>,
        target: PathBuf,
    ) -> Box<Future<Item = (), Error = Error> + Send> {
        let pretty_print = self.pretty_print;
        let force1 = self.force.clone();
        let force2 = self.force.clone();

        debug!("exporter: {:?}", self);

        let setup_target = future::lazy(move || {
            if !target.exists() {
                info!("creating directory {}", target.display());
                create_dir(target.clone())
                    .map_err(|e| e.into())
                    .map(|_| target)
            } else {
                read_dir(target.clone())
                    .map_err(|e| e.into())
                    .and_then(|mut entries| {
                        if let Some(_) = entries.next() {
                            if force1 {
                                warn!("removing and re-creating target dir: {:?}", target.clone());
                                remove_dir_all(target.clone())
                                    .and_then(|_| create_dir(target.clone()))
                                    .map_err(|e| e.into())
                                    .map(|_| target)
                            } else {
                                Err(format_err!(
                                    "target directory {:?} already exists!",
                                    target.display()
                                ))
                            }
                        } else {
                            Ok(target)
                        }
                    })
            }
        });

        let run_export = move |target: PathBuf| {
            Box::new(
                future::join_all(messages.into_iter().map(move |msg| {
                    let target = target.join(&format!("{}.json", msg.id));

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
                })).map(|_| ()),
            )
        };

        Box::new(setup_target.and_then(|target| run_export(target.into())))
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;
    use self::tempdir::TempDir;
    use es::test::MessageBuilder;
    use es::StoredMessage;
    use failure::Error;
    use fs::*;
    use std::fs::File;
    use std::path::PathBuf;
    use tokio::runtime::Runtime;

    fn test_docs() -> Vec<StoredMessage> {
        let doc1 =
            MessageBuilder::published_on("a".to_string(), "test-exchange".to_string()).build();
        let doc2 =
            MessageBuilder::published_on("b".to_string(), "test-exchange".to_string()).build();
        let doc3 =
            MessageBuilder::published_on("c".to_string(), "test-exchange".to_string()).build();
        vec![doc1, doc2, doc3]
    }

    fn assert_export_succeeds(
        result: Result<(), Error>,
        docs: Vec<StoredMessage>,
        target: PathBuf,
    ) {
        assert!(result.is_ok());
        for doc in docs {
            let file_path = target.join(format!("{}.json", doc.id.clone()));
            let msg_file = File::open(file_path.clone())
                .expect(&format!("Cannot open file: {:?}", file_path.display()));
            let parsed_doc: StoredMessage = serde_json::from_reader(msg_file)
                .expect(&format!("Cannot parse file: {:?}", file_path.display()));
            assert_eq!(parsed_doc, doc);
        }
    }

    #[test]
    fn test_exports_messages_when_target_does_not_exist() {
        let target = TempDir::new("export-dir")
            .expect("cannot create tmpdir!")
            .into_path();

        let docs = test_docs();

        let mut rt = Runtime::new().unwrap();
        let exporter = Exporter::new(false, false);

        assert_export_succeeds(
            rt.block_on(exporter.export_messages(docs.to_vec(), target.clone())),
            docs,
            target,
        );
    }

    #[test]
    fn test_fails_export_when_target_exists() {
        let target = TempDir::new("export-dir")
            .expect("cannot create tmpdir!")
            .into_path();
        File::create(target.join("some-file.txt")).expect("Cannot write file!");

        let docs = test_docs();

        let mut rt = Runtime::new().unwrap();
        let exporter = Exporter::new(false, false);

        let error = rt
            .block_on(exporter.export_messages(docs.to_vec(), target.clone()))
            .unwrap_err();
        let error_msg = format!("{}", error);
        assert!(error_msg.contains("already exists"));
    }

    #[test]
    fn test_force_exports_messages_when_target_exist() {
        let target = TempDir::new("other-export-dir")
            .expect("cannot create tmpdir!")
            .into_path();
        File::create(target.join("some-file.txt")).expect("Cannot write file!");

        let docs = test_docs();
        let force = true;

        let mut rt = Runtime::new().unwrap();
        let exporter = Exporter::new(false, force);

        assert_export_succeeds(
            rt.block_on(exporter.export_messages(docs.to_vec(), target.clone())),
            docs,
            target,
        );
    }

}
