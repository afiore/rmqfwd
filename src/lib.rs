#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate lazy_static;

extern crate serde;
extern crate serde_json;
extern crate tokio;
extern crate futures;
extern crate lapin_futures as lapin;
extern crate rs_es;
extern crate chrono;

pub mod rmq;
pub mod es;