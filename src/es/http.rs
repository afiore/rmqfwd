use failure::Error;
use futures::{Future, Stream};
use hyper::client::HttpConnector;
use hyper::Body;
use hyper::Chunk;
use hyper::Client;
use hyper::Request;
use hyper::StatusCode;
use serde_json;

use serde::de::DeserializeOwned;

fn http_err<A: DeserializeOwned + Send + 'static>(
    status: StatusCode,
    body: &Chunk,
) -> Result<A, Error> {
    let body = String::from_utf8_lossy(&body.to_vec()).to_string();
    Err(format_err!(
        "Elasticsearch responded with non successful status code: {:?}. Message: {}",
        status,
        body
    ))
}

pub fn expect_ok(
    client: &Client<HttpConnector, Body>,
    req: Request<Body>,
) -> Box<Future<Item = (), Error = Error> + Send> {
    handle_response(client, req, |status_code, s| match status_code {
        _ if status_code.is_success() => Ok(()),
        _ => http_err(status_code, &s),
    })
}

pub fn is_ok(
    client: &Client<HttpConnector, Body>,
    req: Request<Body>,
) -> Box<Future<Item = bool, Error = Error> + Send> {
    handle_response(client, req, |status_code, _| Ok(status_code.is_success()))
}

pub fn expect_option<A: DeserializeOwned + Send + 'static>(
    client: &Client<HttpConnector, Body>,
    req: Request<Body>,
) -> Box<Future<Item = Option<A>, Error = Error> + Send> {
    handle_response(client, req, |status_code, s| match status_code {
        _ if status_code.is_success() => serde_json::from_slice(s).map(Some).map_err(|e| e.into()),
        _ if status_code.as_u16() == 404 => Ok(None),
        _ => http_err(status_code, &s),
    })
}

pub fn expect<A: DeserializeOwned + Send + 'static>(
    client: &Client<HttpConnector, Body>,
    req: Request<Body>,
) -> Box<Future<Item = A, Error = Error> + Send> {
    handle_response(client, req, |status_code, s| match status_code {
        _ if status_code.is_success() => serde_json::from_slice(s).map_err(|e| e.into()),
        _ => http_err(status_code, &s),
    })
}

fn handle_response<A, F>(
    client: &Client<HttpConnector, Body>,
    req: Request<Body>,
    handle_body: F,
) -> Box<Future<Item = A, Error = Error> + Send>
where
    A: DeserializeOwned + Send + 'static,
    F: Fn(StatusCode, &Chunk) -> Result<A, Error> + Send + Sync + 'static,
{
    Box::new(
        client
            .request(req)
            .and_then(|res| {
                let status = res.status();
                res.into_body()
                    .concat2()
                    .map(move |chunks| (status, chunks))
            })
            .map_err(|e| e.into())
            .and_then(move |(status, body)| handle_body(status, &body)),
    )
}
