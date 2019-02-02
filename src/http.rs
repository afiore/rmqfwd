use futures::{future, Future};

use crate::es::query::MessageQuery;
use crate::es::{FilteredQuery, MessageSearchService, MessageStore};
use hyper::error::Error;
use hyper::{header, Body, Method, Request, Response, StatusCode};
use serde_json;
use std::collections::HashMap;
use std::marker::Send;
use std::sync::{Arc, Mutex};
use try_from::TryFrom;
use url::Url;

type FutureResponse = Box<Future<Item = Response<Body>, Error = Error> + Send>;

pub fn routes(
    msg_store: &Arc<Mutex<MessageStore>>,
    req: Request<Body>,
) -> Box<Future<Item = Response<Body>, Error = Error> + Send> {
    let uri = Url::parse(&(format!("http://somehost{}", req.uri().to_string())))
        .expect("couldn't parse request url!");

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            let msg_store = msg_store.clone();
            let mut params = HashMap::new();
            let query_pairs = uri.query_pairs();

            for (k, v) in query_pairs {
                params.insert(k.to_owned().to_string(), v.to_owned().to_string());
            }

            match TryFrom::try_from(params) {
                Err(e) => {
                    let response_body =
                        json!({"status": 400, "description": format!("bad request: {}", e)})
                            .to_string();

                    let resp: FutureResponse = Box::new(future::ok(
                        Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .header(header::CONTENT_TYPE, "application/json")
                            .header(header::CONTENT_LENGTH, response_body.len() as u64)
                            .body(Body::from(response_body))
                            .unwrap(),
                    ));
                    resp
                }
                Ok(query) => {
                    let mut fq: FilteredQuery = query;
                    //TODO: re-enable
                    fq.aggregate_terms = false;

                    let msg_store = msg_store.lock().unwrap();
                    Box::new(msg_store.search(MessageQuery::Filtered(fq)).then(|results| {
                            match results {
                                Ok(docs) => Ok(Response::builder()
                                    .header(header::CONTENT_TYPE, "application/json")
                                    .header("Access-Control-Allow-Origin", "*") 
                                    .body(Body::from(serde_json::to_string(&docs).unwrap()))
                                    .unwrap()),
                                Err(e) => {
                                    let response_body =
                                    json!({"status": 500, "description": format!("something went wrong: {}", e)})
                                        .to_string();

                                    Ok(Response::builder()
                                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                                        .header(header::CONTENT_TYPE, "application/json")
                                        .header(header::CONTENT_LENGTH, response_body.len() as u64)
                                        .body(Body::from(response_body))
                                        .unwrap())
                                }
                            }
                        }))
                }
            }
        }

        _ => Box::new(future::ok(
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap(),
        )),
    }
}
