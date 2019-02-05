//TODO: consider using nightly
extern crate try_from;

use crate::TimeRange;
use crate::TimeRangeError;
use failure::Error;
use opt::Filters;
use serde_json::Value;
use std::collections::HashMap;
use try_from::TryFrom;

#[derive(Debug)]
pub struct FilteredQuery {
    pub exchange: Option<String>,
    pub body: Option<String>,
    pub routing_key: Option<String>,
    pub time_range: Option<TimeRange>,
    pub exclude_replayed: bool,
    pub from: usize,
    pub aggregate_terms: bool,
}

#[derive(Debug)]
pub enum MessageQuery {
    Filtered(FilteredQuery),
    Ids(Vec<String>),
}

impl TryFrom<Filters> for FilteredQuery {
    type Err = Error;

    fn try_from(f: Filters) -> Result<Self, Error> {
        println!("getting a hashmap");
        let hm: HashMap<String, Vec<String>> = f.into();
        TryFrom::try_from(hm)
    }
}

fn first_val(h: &HashMap<String, Vec<String>>, k: &str) -> Option<String> {
    h.get(k).and_then(|v| v.iter().next().map(|s| s.to_owned()))
}

impl TryFrom<HashMap<String, Vec<String>>> for FilteredQuery {
    type Err = Error;

    fn try_from(h: HashMap<String, Vec<String>>) -> Result<Self, Error> {
        let mut query = MessageQueryBuilder::default();

        if let Some(exchange) = first_val(&h, "exchange") {
            query = query.with_exchange(&exchange);
        }

        if let Some(routing_key) = first_val(&h, "routing-key") {
            query = query.with_routing_key(&routing_key);
        }

        if let Some(body) = first_val(&h, "message-body") {
            query = query.with_body(&body);
        }

        let time_range_result =
            try_from::TryFrom::try_from((first_val(&h, "since"), first_val(&h, "until")));

        let time_range = match time_range_result {
            Ok(time_range) => Some(time_range),
            Err(TimeRangeError::NoInputSupplied) => None,
            Err(TimeRangeError::InvalidFormat { supplied }) => {
                return Err(format_err!("Couldn't parse a time range from {}", supplied));
            }
        };

        if let Some(time_range) = time_range {
            query = query.with_time_range(time_range);
        }
        Ok(query.build())
    }
}

impl TryFrom<Filters> for MessageQuery {
    type Err = Error;
    fn try_from(fs: Filters) -> Result<Self, Error> {
        if !fs.id.is_empty() {
            Ok(MessageQuery::Ids(fs.id))
        } else {
            let query: FilteredQuery = TryFrom::try_from(fs)?;
            Ok(MessageQuery::Filtered(query))
        }
    }
}

fn merge(a: &mut Value, b: &Value) {
    match (a, b) {
        (&mut Value::Object(ref mut a), &Value::Object(ref b)) => {
            for (k, v) in b {
                merge(a.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (a, b) => {
            *a = b.clone();
        }
    }
}

impl FilteredQuery {
    pub fn as_json(&self, es_major_version: u8) -> Value {
        let wrap_if_es2 = move |json: Value| {
            if 2 == es_major_version {
                json!({ "query": json })
            } else {
                json
            }
        };

        let mut filters = vec![wrap_if_es2(json!({
            "match": {
                 "replayed": !self.exclude_replayed
             }
        }))];

        if let Some(exchange) = &self.exchange {
            filters.push(wrap_if_es2(json!(
              {"match": {"exchange": exchange }}
            )));
        }

        if let Some(key) = &self.routing_key {
            filters.push(wrap_if_es2(json!({
              "match": {"routing_key": key }
            })));
        }

        if let Some(body) = &self.body {
            filters.push(wrap_if_es2(json!({
              "match": {"body": body }
            })));
        }

        if let Some(time_range) = &self.time_range {
            let fmt = "%Y-%m-%d %H:%M:%S";
            let es_fmt = "yyyy-MM-dd HH:mm:ss";
            let filter = match time_range {
                TimeRange::Within(start, end) => json!({
                    "gte": start.format(fmt).to_string(),
                    "lte": end.format(fmt).to_string(),
                    "format": es_fmt
                }),
                TimeRange::Since(start) => json!({
                    "gte": start.format(fmt).to_string(),
                    "format": es_fmt
                }),
                TimeRange::Until(end) => json!({
                    "lte": end.format(fmt).to_string(),
                    "format": es_fmt
                }),
            };

            filters.push(json!({
                "range": {
                    "received_at": filter
                }
            }));
        }

        let mut obj = json!({
            "from": self.from,
            "size": 25,
            "query": {
                 "bool": {
                     "must": filters
                 }
            }
        });

        if self.aggregate_terms {
            merge(
                &mut obj,
                &json!({
                //TODO: fix
                "aggs": {
                  "message": {
                     "aggs": {
                         "exchange": {
                             "terms": { "field": "exchange" }
                         },
                         "routing_key": {
                             "terms": { "field": "routing_key" }
                         }
                     }
                }}}),
            );
        }

        info!("returning query: {}", obj);

        obj
    }
}

pub struct MessageQueryBuilder {
    query: FilteredQuery,
}

impl Default for MessageQueryBuilder {
    fn default() -> Self {
        MessageQueryBuilder {
            query: FilteredQuery {
                exchange: None,
                body: None,
                routing_key: None,
                time_range: None,
                exclude_replayed: true,
                from: 0,
                aggregate_terms: false,
            },
        }
    }
}

impl MessageQueryBuilder {
    pub fn with_exchange(mut self, exchange: &str) -> Self {
        self.query.exchange = Some(exchange.to_owned());
        self
    }

    pub fn with_routing_key(mut self, key: &str) -> Self {
        self.query.routing_key = Some(key.to_owned());
        self
    }

    pub fn with_time_range(mut self, time_range: TimeRange) -> Self {
        self.query.time_range = Some(time_range);
        self
    }

    pub fn with_body(mut self, body: &str) -> Self {
        self.query.body = Some(body.to_owned());
        self
    }

    pub fn from(mut self, from: usize) -> Self {
        self.query.from = from;
        self
    }

    pub fn aggreating_terms(mut self) -> Self {
        self.query.aggregate_terms = true;
        self
    }

    pub fn build(self) -> FilteredQuery {
        self.query
    }
}
