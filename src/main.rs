extern crate rmqfwd;
extern crate futures;
extern crate tokio;
extern crate env_logger;

use rmqfwd::rmq::{Config, Message};
use rmqfwd::rmq;
use rmqfwd::es::{MessageSearch, MessageSearchService};
use rmqfwd::es;
use futures::sync::mpsc;
use tokio::runtime::Runtime;

fn main() {
    env_logger::init();

    let (tx, rx) =  mpsc::channel::<Message>(5);
    let mut rt = Runtime::new().unwrap();
    let msg_search = MessageSearch::new(es::Config::default());

    rt.block_on(msg_search.init_index()).expect("MessageSearch.init_index() failed!");
    rt.spawn(msg_search.write(rx));
    rt.block_on(rmq::bind_and_consume(Config::default(), tx)).expect("runtime error!");
}
