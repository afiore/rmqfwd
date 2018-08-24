extern crate rmqfwd;
extern crate futures;
extern crate tokio;

use rmqfwd::rmq::{Config, Message};
use rmqfwd::rmq;
use rmqfwd::es;

use futures::sync::mpsc;
use tokio::runtime::Runtime;

fn main() {
    let (tx, rx) =  mpsc::channel::<Message>(5);
    let mut rt = Runtime::new().unwrap();

    rt.spawn(es::write_messages(rx, es::Config::default()));

    rt.block_on(rmq::bind_and_consume(Config::default(), tx)).expect("runtime error!");
}
