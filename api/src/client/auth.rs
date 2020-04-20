use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use tonic::{transport::Uri, Request, Status};

use biscuit::jwa::SignatureAlgorithm;
use biscuit::jwk::RSAKeyParameters;
use biscuit::jws::{Header, RegisteredHeader, Secret as JwsSecret};
use biscuit::{
    ClaimsSet, Empty, RegisteredClaims, SingleOrMultiple, StringOrUri, JWT as BiscuitJWT,
};
pub type JWT = BiscuitJWT<Empty, Empty>;

use chrono::{DateTime, Duration, Utc};

use ring::signature::RsaKeyPair;

use super::BuilderError;

pub enum AuthFlow {
    Direct(DirectFlow),
    Oauth(OauthFlow),
}

pub struct DirectFlow {
    audience: StringOrUri,
}

#[derive(Debug)]
pub enum AuthFlowError {
    Biscuit(biscuit::errors::Error),
}

impl std::fmt::Display for AuthFlowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Biscuit(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for AuthFlowError {}

impl AuthFlow {
    pub async fn get_token(
        &self,
        secret: &Secret,
    ) -> Result<(String, DateTime<Utc>), AuthFlowError> {
        match self {
            Self::Direct(DirectFlow { audience }) => self
                .new_default_signed(audience, secret)
                .map_err(|e| AuthFlowError::Biscuit(e)),
            Self::Oauth(..) => unimplemented!("oauth flow"),
        }
    }
    pub fn new_default_signed(
        &self,
        audience: &StringOrUri,
        secret: &Secret,
    ) -> Result<(String, DateTime<Utc>), biscuit::errors::Error> {
        let header = Header {
            registered: RegisteredHeader {
                algorithm: SignatureAlgorithm::RS256,
                ..Default::default()
            },
            private: Empty {},
        };

        let issued_at = Utc::now();
        let expiry = issued_at + Duration::hours(1);

        let claims_set = ClaimsSet {
            registered: RegisteredClaims {
                issuer: Some(secret.user_name.clone()),
                subject: Some(secret.user_name.clone()),
                issued_at: Some(issued_at.into()),
                expiry: Some(expiry.clone().into()),
                audience: Some(SingleOrMultiple::Single(audience.clone())),
                ..Default::default()
            },
            private: Empty {},
        };

        let jwt = JWT::new_decoded(header, claims_set);

        let encoded = jwt.encode(&secret.secret_key)?.unwrap_encoded().to_string();
        Ok((encoded, expiry))
    }
}

pub struct OauthFlow {
    oauth_endpoint: Uri,
}

pub struct Authenticator {
    secret: Secret,
    auth_flow: AuthFlow,
    storage: Mutex<Storage>,
}

pub struct Secret {
    user_name: StringOrUri,
    secret_key: JwsSecret,
}

pub struct Storage {
    token: Option<String>,
    expires_at: DateTime<Utc>,
}

impl Default for Storage {
    fn default() -> Self {
        Self {
            token: None,
            expires_at: Utc::now(),
        }
    }
}

impl Storage {
    fn get_token(&self) -> Option<&str> {
        if Utc::now() + Duration::seconds(5) > self.expires_at {
            None
        } else {
            self.token.as_ref().map(|s| s.as_str())
        }
    }
    fn set_token(&mut self, token: String, expires_at: DateTime<Utc>) {
        self.token = Some(token);
        self.expires_at = expires_at;
    }
}

impl Authenticator {
    pub fn builder() -> AuthenticatorBuilder {
        AuthenticatorBuilder::default()
    }
    pub fn new(auth_flow: AuthFlow, secret: Secret) -> Self {
        Self {
            secret,
            auth_flow,
            storage: Mutex::new(Storage::default()),
        }
    }
    pub async fn authorize<R>(&self, mut req: Request<R>) -> Result<Request<R>, Status> {
        let token = {
            let mut storage = self.storage.lock().unwrap();
            match storage.get_token() {
                Some(token) => token.to_string(),
                None => {
                    let (token, expires_at) = self.refresh().await?;
                    storage.set_token(token.clone(), expires_at);
                    token
                }
            }
        };
        req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        Ok(req)
    }
    pub async fn refresh(&self) -> Result<(String, DateTime<Utc>), Status> {
        self.auth_flow
            .get_token(&self.secret)
            .await
            .map_err(|e| Status::unauthenticated(e.to_string()))
    }
}

pub enum AuthFlowKind {
    Direct,
    Oauth,
}

#[derive(Default)]
pub struct AuthenticatorBuilder {
    secret_key: Option<Vec<u8>>,
    user_name: Option<String>,
    audience: Option<String>,
    auth_flow_kind: Option<AuthFlowKind>,
}

impl AuthenticatorBuilder {
    /// Set the username used for authentication.
    pub fn user_name(mut self, user_name: &str) -> Self {
        self.user_name = Some(user_name.to_string());
        self
    }
    pub fn auth_flow(mut self, kind: AuthFlowKind) -> Self {
        self.auth_flow_kind = Some(kind);
        self
    }
    pub fn auth_flow_direct(self) -> Self {
        self.auth_flow(AuthFlowKind::Direct)
    }
    pub fn secret_key(mut self, bytes: &[u8]) -> Self {
        self.secret_key = Some(bytes.iter().cloned().collect());
        self
    }
    pub fn audience(mut self, audience: &str) -> Self {
        self.audience = Some(audience.to_string());
        self
    }
    pub fn build(self) -> Result<Authenticator, BuilderError> {
        let secret_key = self
            .secret_key
            .ok_or(BuilderError::Required("secret_key"))?;
        let from_der = RsaKeyPair::from_der(secret_key.as_slice())
            .map_err(|e| BuilderError::KeyRejected(e))?;
        let jws_secret = JwsSecret::RsaKeyPair(Arc::new(from_der));

        let user_name = StringOrUri::from_str(
            self.user_name
                .as_ref()
                .map(|s| s.as_str())
                .ok_or(BuilderError::Required("user_name"))?,
        )
        .map_err(|e| BuilderError::Invalid("user_name", e.to_string()))?;

        let audience = StringOrUri::from_str(
            self.user_name
                .as_ref()
                .map(|s| s.as_str())
                .unwrap_or("parallax.openquery.io"),
        )
        .map_err(|e| BuilderError::Invalid("audience", e.to_string()))?;

        let auth_flow_kind = self.auth_flow_kind.unwrap_or(AuthFlowKind::Direct);
        let auth_flow = match auth_flow_kind {
            AuthFlowKind::Direct => AuthFlow::Direct(DirectFlow { audience }),
            AuthFlowKind::Oauth => unimplemented!("oauth flow"),
        };

        let secret = Secret {
            user_name,
            secret_key: jws_secret,
        };

        Ok(Authenticator::new(auth_flow, secret))
    }
}

pub fn generate_rsa_key_pair_pem() -> std::io::Result<String> {
    let child = Command::new("openssl")
        .arg("genpkey")
        .arg("-algorithm")
        .arg("RSA")
        .arg("-pkeyopt")
        .arg("rsa_keygen_bits:2048")
        .arg("-pkeyopt")
        .arg("rsa_keygen_pubexp:65537")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut pkey = String::new();
    child.stdout.unwrap().read_to_string(&mut pkey);
    Ok(pkey)
}

pub fn rsa_key_pem_to_der(pem: &str) -> std::io::Result<Vec<u8>> {
    let child = Command::new("openssl")
        .arg("rsa")
        .arg("-outform")
        .arg("der")
        .stdout(Stdio::piped())
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child.stdin.unwrap().write(pem.as_bytes())?;

    let mut der = Vec::new();
    child.stdout.unwrap().read_to_end(&mut der)?;
    Ok(der)
}

pub fn extract_public_key_pem(pem: &[u8]) -> std::io::Result<String> {
    let child = Command::new("openssl")
        .arg("rsa")
        .arg("-pubout")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child.stdin.unwrap().write(&pem)?;

    let mut public_key = String::new();
    child.stdout.unwrap().read_to_string(&mut public_key)?;

    Ok(public_key)
}
