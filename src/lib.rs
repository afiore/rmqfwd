#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate serde_json;
#[macro_use] extern crate failure;

extern crate url;
extern crate hyper;
extern crate chrono;
extern crate futures;
extern crate lapin_futures as lapin;
extern crate serde;
extern crate tokio;
extern crate tokio_codec;
extern crate clap;

pub mod es;
pub mod rmq;
pub mod fs;

use chrono::prelude::*;

#[derive(Debug)]
pub enum TimeRange {
    StartEnd { start: DateTime<Utc>, end: DateTime<Utc> },
    Since(DateTime<Utc>),
    Until(DateTime<Utc>),
}
