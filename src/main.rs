extern crate rmqfwd;
extern crate futures;

use rmqfwd::rmq::Config;
use rmqfwd::rmq::bind_and_consume;
use futures::sync::mpsc;

fn main() {
    let (tx, _rx) =  mpsc::channel(5);
    bind_and_consume(Config::default(), tx);
}
