extern crate rmqfwd;

use rmqfwd::rmq::Config;
use rmqfwd::rmq::bind_and_consume;

fn main() {
    bind_and_consume(Config::default());
}
