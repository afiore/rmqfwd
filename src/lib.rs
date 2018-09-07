#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate maplit;
#[macro_use]
extern crate serde_json;

extern crate chrono;
extern crate futures;
extern crate lapin_futures as lapin;
extern crate rs_es;
extern crate serde;
extern crate tokio;
extern crate uuid;

pub mod es;
pub mod rmq;
