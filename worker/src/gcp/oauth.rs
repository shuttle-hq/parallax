use serde::Deserialize;

use std::convert::TryInto;
use std::env;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use futures::stream::StreamExt;
use futures_01::{future::FutureResult, Future};

use yup_oauth2::{
    service_account_key_from_file, ApplicationSecret, GetToken, RequestError, ServiceAccountAccess,
    Token,
};

use hyper::{Body, Request, StatusCode};

use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::gcp::{errors::*, CollectBody};
use chrono::{Timelike, Utc};

static METADATA_URL: &'static str = "http://metadata.google.internal/computeMetadata\
                                     /v1/instance/service-accounts/default";

pub enum Connector<N = ()> {
    Native(N),
    Env(EnvironmentAccess),
}

type DefaultClient = hyper::Client<hyper::client::HttpConnector>;

#[derive(Deserialize)]
pub struct GoogleToken {
    access_token: String,
    expires_in: u64,
    token_type: String,
}

pub struct EnvironmentAccess<C = DefaultClient> {
    client: C,
    scopes: Vec<String>,
    token: Option<Token>,
}

impl EnvironmentAccess<DefaultClient> {
    pub fn new() -> Self {
        let client = hyper::Client::default();
        Self {
            client: client,
            scopes: vec![],
            token: None,
        }
    }
    async fn get_metadata<T>(&self, endpoint: &str) -> Result<T>
    where
        for<'a> T: Deserialize<'a>,
    {
        let token_uri = format!("{}/{}", METADATA_URL, endpoint);

        let req = Request::builder()
            .method("GET")
            .uri(token_uri)
            .header("Metadata-Flavor", "Google")
            .body(Body::empty())?;

        let resp = self.client.request(req).await?;

        let status = resp.status();

        let body = CollectBody::from(resp).collect().await?;

        if status == StatusCode::OK {
            Ok(serde_json::from_slice(&body)?)
        } else {
            Err(GcpError::ApiError("could not get_metadata".to_string()))
        }
    }
    async fn refresh(&mut self) -> Result<()> {
        let google_token: GoogleToken = self.get_metadata("token").await?;
        let now = Utc::now().timestamp();
        let token = Token {
            access_token: google_token.access_token,
            refresh_token: String::new(),
            token_type: google_token.token_type,
            expires_in: Some(google_token.expires_in as i64),
            expires_in_timestamp: Some(((google_token.expires_in) as i64) + now),
        };
        let scopes: Vec<String> = self.get_metadata("scopes").await?;
        self.token = Some(token);
        self.scopes = scopes;
        Ok(())
    }
}

impl GetToken for EnvironmentAccess {
    fn token<I, T>(
        &mut self,
        scopes: I,
    ) -> Box<dyn Future<Item = Token, Error = RequestError> + Send> {
        let token = self
            .token
            .as_ref()
            .ok_or(RequestError::InvalidClient)
            .and_then(|t| Ok(t.clone()));
        FutureResult::from(token).boxed()
    }
    fn api_key(&mut self) -> Option<String> {
        None
    }
    fn application_secret(&self) -> ApplicationSecret {
        ApplicationSecret::default()
    }
}

impl Connector<()> {
    pub fn builder() -> Builder {
        Builder::default()
    }
}

impl<N> GetToken for Connector<N>
where
    N: GetToken,
{
    fn token<I, T>(
        &mut self,
        scopes: I,
    ) -> Box<dyn Future<Item = Token, Error = RequestError> + Send>
    where
        T: Into<String>,
        I: IntoIterator<Item = T>,
    {
        match self {
            Connector::Native(gt) => gt.token(scopes),
            Connector::Env(env) => env.token(scopes),
        }
    }
    fn api_key(&mut self) -> Option<String> {
        match self {
            Connector::Native(sa) => sa.api_key(),
            Connector::Env(env) => env.api_key(),
        }
    }
    fn application_secret(&self) -> ApplicationSecret {
        match self {
            Connector::Native(sa) => sa.application_secret(),
            Connector::Env(env) => env.application_secret(),
        }
    }
}

pub enum SecretSource {
    File(PathBuf),
    Key(String),
}

pub struct Builder {
    sa_key: Option<SecretSource>,
    allow_from_metadata: bool,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            sa_key: None,
            allow_from_metadata: false,
        }
    }
}

impl Builder {
    pub fn with_key_file(mut self, path: &Path) -> Self {
        self.sa_key = Some(SecretSource::File(PathBuf::from(path)));
        self
    }
    pub fn with_secret_key(mut self, key: &str) -> Self {
        self.sa_key = Some(SecretSource::Key(key.to_string()));
        self
    }
    pub fn set_allow_from_metadata(mut self, allow: bool) -> Self {
        self.allow_from_metadata = allow;
        self
    }
    pub fn build(self) -> io::Result<impl GetToken> {
        let allow_from_metadata = self.allow_from_metadata;
        if let Some(secret) = self.sa_key {
            let service_account_key = match secret {
                SecretSource::File(f) => service_account_key_from_file(&f.as_path()),
                SecretSource::Key(k) => {
                    serde_json::from_str(&k).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                }
            }?;
            let auth = ServiceAccountAccess::new(service_account_key).build();
            Ok(Connector::Native(auth))
        } else {
            if allow_from_metadata {
                Ok(Connector::Env(EnvironmentAccess::new()))
            } else {
                let err_msg =
                    "defaulted to env access but it was not explicitly permitted".to_string();
                Err(io::Error::new(io::ErrorKind::InvalidInput, err_msg))
            }
        }
    }
}
