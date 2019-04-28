use crate::es::StoredMessage;
use crate::opt::{EsConfig, RmqConfig};
use failure::Error;
use futures::{future, Future};
use serde_json;
use std::convert::AsRef;
use std::fs::{create_dir, read_dir, remove_dir_all};
use std::io::Read;
use std::path::{Path, PathBuf};
use tokio::fs::file::File;
use toml;

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
            pretty_print,
            force,
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
        let force1 = self.force;
        let force2 = self.force;

        debug!("exporter: {:?}", self);

        let setup_target = future::lazy(move || {
            if !target.exists() {
                info!("creating directory {}", &target.display());
                create_dir(target.clone())
                    .map_err(|e| e.into())
                    .map(|_| target)
            } else {
                read_dir(target.clone())
                    .map_err(|e| e.into())
                    .and_then(|mut entries| {
                        if entries.next().is_some() {
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
                    })
                    .and_then(|target| {
                        info!("exporting {:?}", target.display());
                        File::create(target).from_err()
                    })
                    .and_then(move |file| {
                        let result = if pretty_print {
                            serde_json::to_writer_pretty(file, &msg)
                        } else {
                            serde_json::to_writer(file, &msg)
                        };

                        result.map_err(|e| e.into())
                    })
                }))
                .map(|_| ()),
            )
        };

        Box::new(setup_target.and_then(run_export))
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub elasticsearch: EsConfig,
    pub rabbitmq: RmqConfig,
}

fn config_reader<A: Read>(source: &mut A) -> Result<Config, Error> {
    let mut buf: Vec<u8> = Vec::new();
    source.read_to_end(&mut buf)?;
    toml::de::from_slice(&buf[..]).map_err(|e| e.into())
}

pub fn read_config<P: AsRef<Path>>(path: P) -> Result<Config, Error> {
    let mut file = std::fs::File::open(path)?;
    let config = config_reader(&mut file)?;
    Ok(config)
}

#[cfg(test)]
mod test {
    extern crate tempdir;
    use self::tempdir::TempDir;
    use crate::es::test::MessageBuilder;
    use crate::es::StoredMessage;
    use crate::fs::*;
    use failure::Error;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use tokio::runtime::Runtime;

    fn test_docs() -> Vec<StoredMessage> {
        let doc1 = MessageBuilder::published_on("a", "test-exchange").build();
        let doc2 = MessageBuilder::published_on("b", "test-exchange").build();
        let doc3 = MessageBuilder::published_on("c", "test-exchange").build();
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

    const TOML: &str = r#"
        [rabbitmq]
        host = "messages.example.com"
        port = 5672
        tracing_exchange = "amqp.rabbitmq.trace"

        [rabbitmq.creds]
        user = "guest"
        password = "guest"

        [elasticsearch]
        index = "rabbit_messages"
        message_type = "message"
        base_url = "http://search.example.com:9200"
        "#;

    #[test]
    fn test_toml_config_from_string() {
        let mut bytes = TOML.as_bytes().clone();
        let config = config_reader(&mut bytes);
        assert!(config.is_ok());
    }

    #[test]
    fn test_toml_config_from_file() {
        let config_path = TempDir::new("test-config")
            .expect("cannot create tmpdir!")
            .into_path()
            .join("some-file.txt");
        let mut file = File::create(&config_path).expect("Cannot write file!");
        file.write_all(TOML.as_bytes().clone()).unwrap();

        assert!(read_config(&config_path).is_ok());
    }

}
