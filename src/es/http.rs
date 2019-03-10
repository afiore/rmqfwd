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

pub mod request {
    use crate::hyper::{header, Body, Method, Request};
    use crate::serde::Serialize;
    use crate::url::Url;
    use futures::stream;

    pub fn get(url: &Url) -> Request<Body> {
        mk_request::<()>(Method::GET, &url, None)
    }
    pub fn head(url: &Url) -> Request<Body> {
        mk_request::<()>(Method::HEAD, &url, None)
    }
    pub fn delete(url: &Url) -> Request<Body> {
        mk_request::<()>(Method::DELETE, &url, None)
    }

    pub fn put<A: Serialize + Send + 'static>(url: &Url, maybe_entity: Option<A>) -> Request<Body> {
        mk_request(Method::PUT, &url, maybe_entity)
    }

    pub fn post<A: Serialize + Send + 'static>(
        url: &Url,
        maybe_entity: Option<A>,
    ) -> Request<Body> {
        mk_request(Method::POST, &url, maybe_entity)
    }

    fn mk_request<A: Serialize + Send + 'static>(
        method: Method,
        url: &Url,
        maybe_entity: Option<A>,
    ) -> Request<Body> {
        let body = maybe_entity
            .map(|entity| Body::wrap_stream(stream::once(serde_json::to_string(&entity))))
            .unwrap_or_else(|| Body::default());
        Request::builder()
            .method(method)
            .header(header::CONTENT_TYPE, "application/json")
            .uri(url.to_string())
            .body(body)
            .expect("couldn't build a request!")
    }

}
