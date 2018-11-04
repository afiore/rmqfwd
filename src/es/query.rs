extern crate try_from;

use clap::ArgMatches;
use failure::Error;
use serde_json::Value;
use try_from::TryFrom;
use TimeRange;
use TimeRangeError;

#[derive(Debug)]
pub struct MessageQuery {
    pub exchange: String,
    pub body: Option<String>,
    pub routing_key: Option<String>,
    pub time_range: Option<TimeRange>,
    pub exclude_replayed: bool,
}

impl<'s, 't> TryFrom<&'s ArgMatches<'t>> for MessageQuery {
    type Err = Error;
    fn try_from(matches: &'s ArgMatches<'t>) -> Result<Self, Error> {
        let exchange = matches.value_of("exchange").unwrap().to_string();
        let routing_key = matches.value_of("routing-key").map(|s| s.to_string());
        let body = matches.value_of("message-body").map(|s| s.to_string());
        let since = matches.value_of("since").map(|s| s.to_string());
        let until = matches.value_of("until").map(|s| s.to_string());

        let mut query = MessageQueryBuilder::with_exchange(&exchange);

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
                return Err(format_err!("Couldn't parse time range from {}", supplied));
            }
        };

        for time_range in time_range {
            query = query.with_time_range(time_range);
        }

        Ok(query.build())
    }
}

impl Into<Value> for MessageQuery {
    fn into(self) -> Value {
        let mut nested: Vec<Value> = Vec::new();

        nested.push(json!(
            {"match": {"message.exchange": self.exchange }}
        ));

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
                "query": {
                    "match": {
                        "replayed": !self.exclude_replayed
                    }
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

        json!({
            "query": {
                 "bool": {
                     "must": filters
                 }
            }
        })
    }
}

pub struct MessageQueryBuilder {
    query: MessageQuery,
}

impl MessageQueryBuilder {
    pub fn with_exchange(exchange: &str) -> Self {
        MessageQueryBuilder {
            query: MessageQuery {
                exchange: exchange.to_owned(),
                body: None,
                routing_key: None,
                time_range: None,
                exclude_replayed: true,
            },
        }
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

    pub fn build(self) -> MessageQuery {
        self.query
    }
}
