#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate failure;
extern crate chrono;
extern crate clap;
extern crate futures;
extern crate hyper;
extern crate lapin_futures as lapin;
extern crate serde;
extern crate tokio;
extern crate tokio_codec;
extern crate try_from;
extern crate url;

pub mod es;
pub mod fs;
pub mod rmq;

use chrono::prelude::*;

use try_from::TryFrom;

#[derive(Debug)]
pub enum TimeRange {
    Within(DateTime<Utc>, DateTime<Utc>),
    Since(DateTime<Utc>),
    Until(DateTime<Utc>),
}

#[derive(Debug, Fail)]
pub enum TimeRangeError {
    #[fail(display = "invalid format: {}", supplied)]
    InvalidFormat { supplied: String },

    #[fail(display = "No Input supplied!")]
    NoInputSupplied,
}

fn convert(s: String) -> Result<DateTime<Utc>, TimeRangeError> {
    s.parse::<DateTime<Utc>>()
        .map_err(|_| TimeRangeError::InvalidFormat { supplied: s })
}

impl TryFrom<(Option<String>, Option<String>)> for TimeRange {
    type Err = TimeRangeError;
    fn try_from(pair: (Option<String>, Option<String>)) -> Result<Self, TimeRangeError> {
        use TimeRangeError::*;
        match pair {
            (Some(d1), Some(d2)) => {
                let d1 = convert(d1)?;
                let d2 = convert(d2)?;
                Ok(TimeRange::Within(d1, d2))
            }
            (_, Some(d2)) => {
                let d2 = convert(d2)?;
                Ok(TimeRange::Until(d2))
            }
            (Some(d1), _) => {
                let d1 = convert(d1)?;
                Ok(TimeRange::Since(d1))
            }
            _ => Err(NoInputSupplied),
        }
    }
}
