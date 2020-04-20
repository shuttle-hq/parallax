use serde::{Deserialize, Serialize};

use hyper::{client::connect::HttpConnector, Body, Request, Response, StatusCode};
use hyper_tls::HttpsConnector;
use yup_oauth2::GetToken;

use http::{
    header::{HeaderValue, AUTHORIZATION},
    method::Method,
    uri::Uri,
};

use futures::compat::Future01CompatExt;
use futures::prelude::*;
use futures::stream::{Stream, StreamExt};
use futures_01::Future;
use std::sync::mpsc::channel;
use yup_oauth2::Token;

use std::convert::TryFrom;
use std::sync::Mutex;

pub mod bigquery;
pub(crate) mod errors;
pub mod oauth;
use errors::*;

pub struct Client<O> {
    oauth: Mutex<O>,
    http: hyper::Client<HttpsConnector<HttpConnector>, Body>,
}

impl<O> Client<O> {
    pub fn new(oauth: O) -> Self {
        let connector = HttpsConnector::new();
        let http = hyper::Client::builder().build(connector);
        Self {
            oauth: Mutex::new(oauth),
            http,
        }
    }
}

pub struct CollectBody {
    inner: Body,
}

impl From<Response<Body>> for CollectBody {
    fn from(resp: Response<Body>) -> Self {
        Self {
            inner: resp.into_body(),
        }
    }
}

impl CollectBody {
    pub async fn collect(self) -> Result<Vec<u8>> {
        let res = self
            .inner
            .fold(Ok(vec![]): Result<Vec<u8>>, |mut st, mc| async move {
                st.and_then(|mut ist| {
                    ist.extend(mc?);
                    Ok(ist)
                })
            })
            .await?;
        Ok(res)
    }
}

fn run_in_legacy_runtime<O>(oauth: &mut O) -> Result<Token>
where
    O: GetToken,
{
    let (tx, rx) = channel::<Token>();

    hyper_legacy::rt::run({
        oauth
            .token(vec!["https://www.googleapis.com/auth/cloud-platform"])
            .map(move |token| {
                tx.send(token);
            })
            .map_err(|e| {
                error!("failed to acquire a gcp token: {:?}", e);
                ()
            })
    });

    let token = rx.recv().map_err(|e| {
        GcpError::AuthError(format!("Failed to acquire GCP Token: {}", e.to_string()))
    })?;

    Ok(token)
}

impl<O> Client<O>
where
    O: GetToken,
{
    async fn auth_header(&self) -> Result<HeaderValue> {
        let token = {
            let mut oauth = self
                .oauth
                .lock()
                .map_err(|e| GcpError::AuthError("Failed to acquire oauth lock".to_string()))?;
            run_in_legacy_runtime(&mut *oauth)
        }?;
        let header_str = format!("Bearer {}", token.access_token);
        let header_val =
            http::header::HeaderValue::from_str(&header_str).map_err(|e| http::Error::from(e))?;
        Ok(header_val)
    }
    async fn api_request<T, P>(&self, method: Method, uri: Uri, body: Option<T>) -> Result<P>
    where
        T: Serialize,
        for<'a> P: Deserialize<'a>,
    {
        let bearer_token = self.auth_header().await?;

        let body = body
            .map(|b| {
                Ok(Body::from(serde_json::to_vec(&b)?)):
                    std::result::Result<Body, serde_json::Error>
            })
            .unwrap_or(Ok(Body::empty()));

        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header(AUTHORIZATION, bearer_token)
            .body(body?)?;

        let resp = self.http.request(req).await?;

        let status = resp.status();

        let resp_body = CollectBody::from(resp).collect().await?;

        if status == StatusCode::OK {
            Ok(serde_json::from_slice(&resp_body)?)
        } else {
            // replace with correct errorproto
            let resp_str =
                String::from_utf8(resp_body).unwrap_or("[invalid response body]".to_string());
            Err(GcpError::ApiError(resp_str))
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use std::path::Path;

    const SERVICE_ACCOUNT_KEY: &'static str = env!("PARALLAX_GCP_SERVICE_ACCOUNT_KEY");

    pub fn mk_connector() -> impl GetToken {
        oauth::Connector::builder()
            .with_key_file(&Path::new(SERVICE_ACCOUNT_KEY))
            .set_allow_from_metadata(false)
            .build()
            .expect("invalid GCP credentials")
    }

    pub fn mk_client() -> Client<impl GetToken> {
        let gcp_client = Client::new(mk_connector());
        gcp_client
    }
}
