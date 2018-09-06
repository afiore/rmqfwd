extern crate rmqfwd;
extern crate futures;
extern crate tokio;
extern crate env_logger;
#[macro_use] extern crate log;

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
    let mut msg_search = MessageSearch::new(es::Config::default());

    //TODO: move elsewhere
/*     match &msg_search.init_index() {
        Ok(()) => info!("initialised es index"),
        Err(err) => {
            error!("couldn't initialise index: {}", err.description());
        }
    }
 */
    rt.spawn(msg_search.write(rx));
    rt.block_on(rmq::bind_and_consume(Config::default(), tx)).expect("runtime error!");
}
