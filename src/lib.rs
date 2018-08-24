#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;
extern crate serde;
extern crate serde_json;
extern crate tokio;
extern crate futures;
extern crate lapin_futures as lapin;
extern crate rs_es;

pub mod rmq;
pub mod es;