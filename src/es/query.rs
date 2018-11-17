//TODO: consider using nightly
extern crate try_from;

use clap::ArgMatches;
use failure::Error;
use serde_json::Value;
use std::collections::HashMap;
use try_from::TryFrom;
use TimeRange;
use TimeRangeError;

#[derive(Debug)]
pub struct MessageQuery {
    pub exchange: Option<String>,
    pub body: Option<String>,
    pub routing_key: Option<String>,
    pub time_range: Option<TimeRange>,
    pub exclude_replayed: bool,
    pub from: usize,
    pub aggregate_terms: bool,
}

impl TryFrom<HashMap<String, String>> for MessageQuery {
    type Err = Error;

    fn try_from(h: HashMap<String, String>) -> Result<Self, Error> {
        let exchange = h.get("exchange");
        let routing_key = h.get("routing-key").map(|s| s.to_string());
        let body = h.get("message-body").map(|s| s.to_string());
        let since = h.get("since").map(|s| s.to_string());
        let until = h.get("until").map(|s| s.to_string());

        let mut query = MessageQueryBuilder::default();

        for exch in exchange {
            query = query.with_exchange(&exch);
        }

        for key in routing_key {
            query = query.with_routing_key(&key);
        }

        for body in body {
            query = query.with_body(&body);
        }

        let time_range_result = try_from::TryFrom::try_from((since, until));

        let time_range = match time_range_result {
            Ok(time_range) => Some(time_range),
            Err(TimeRangeError::NoInputSupplied) => None,
            Err(TimeRangeError::InvalidFormat { supplied }) => {
                return Err(format_err!("Couldn't parse a time range from {}", supplied));
            }
        };

        for time_range in time_range {
            query = query.with_time_range(time_range);
        }
        Ok(query.build())
    }
}

impl<'s, 't> TryFrom<&'s ArgMatches<'t>> for MessageQuery {
    type Err = Error;
    fn try_from(matches: &'s ArgMatches<'t>) -> Result<Self, Error> {
        let mut h: HashMap<String, String> = HashMap::new();
        for (k, v) in matches.args.iter() {
            if v.vals.len() > 0 {
                //TODO: is there a safer way to convert OsString to String?
                h.insert(k.to_string(), v.vals[0].clone().into_string().unwrap());
            }
        }
        TryFrom::try_from(h)
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

impl Into<Value> for MessageQuery {
    fn into(self) -> Value {
        let mut nested: Vec<Value> = Vec::new();

        for exchange in self.exchange {
            nested.push(json!(
              {"match": {"message.exchange": exchange }}
            ));
        }

        for key in self.routing_key {
            nested.push(json!({
              "match": {"message.routing_key": key }
            }));
        }
        for body in self.body {
            nested.push(json!({
              "match": {"message.body": body }
            }));
        }

        let nested_obj = json!({
            "nested": {
                "path": "message",
                "score_mode": "avg",
                "query": {
                    "bool": {
                        "must": nested
                    }
                }
        }});

        let mut filters = vec![
            nested_obj,
            json!({
                "match": {
                     "replayed": !self.exclude_replayed
                 }
            }),
        ];

        for time_range in self.time_range {
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
                &json!({"aggs": {
                       "message": {
                          "nested" : {
                              "path" : "message"
                          },
                          "aggs": {
                          	"exchange": {
                          		"terms": { "field": "message.exchange" }
                          	},
                          	"routing_key": {
                          		"terms": { "field": "message.routing_key" }
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
    query: MessageQuery,
}

impl Default for MessageQueryBuilder {
    fn default() -> Self {
        MessageQueryBuilder {
            query: MessageQuery {
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

    pub fn build(self) -> MessageQuery {
        self.query
    }
}
