use std::convert::TryInto;
use std::net::IpAddr;

pub use crate::tonic::transport::ClientTlsConfig;
use crate::tonic::{
    transport::{Channel, Endpoint},
    IntoRequest, Response, Status, Streaming,
};
use http::uri::InvalidUri;

use crate::{JobServiceClient, ResourceServiceClient};

use crate::proto::parallax::service::job::v1::*;
use crate::proto::parallax::service::resource::v1::*;

pub mod auth;
pub use auth::{
    extract_public_key_pem, generate_rsa_key_pair_pem, rsa_key_pem_to_der, Authenticator,
};

pub const DEFAULT_JOB_PORT: u16 = 6477;
pub const DEFAULT_RESOURCE_PORT: u16 = 6477;

#[derive(Debug)]
pub enum BuilderError {
    Tonic(tonic::transport::Error),
    KeyRejected(ring::error::KeyRejected),
    Required(&'static str),
    Invalid(&'static str, String),
}

impl std::fmt::Display for BuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tonic(e) => write!(f, "tonic: {}", e),
            Self::KeyRejected(e) => write!(f, "key rejected: {}", e),
            Self::Required(r) => write!(f, "required: {}", r),
            Self::Invalid(inv, reason) => write!(f, "invalid {}: {}", inv, reason),
        }
    }
}

impl std::error::Error for BuilderError {}

#[derive(Default)]
pub struct ClientBuilder {
    job_host: Option<String>,
    job_port: Option<u16>,
    resource_host: Option<String>,
    resource_port: Option<u16>,
    authenticator: Option<Authenticator>,
    client_tls_config: Option<ClientTlsConfig>,
    disable_tls: bool,
}

impl ClientBuilder {
    pub fn job_host<A: AsRef<str>>(mut self, addr: A) -> Self {
        self.job_host = Some(addr.as_ref().to_string());
        self
    }
    pub fn job_port(mut self, port: u16) -> Self {
        self.job_port = Some(port);
        self
    }
    pub fn resource_host<A: AsRef<str>>(mut self, addr: A) -> Self {
        self.resource_host = Some(addr.as_ref().to_string());
        self
    }
    pub fn resource_port(mut self, port: u16) -> Self {
        self.resource_port = Some(port);
        self
    }
    /// Sets both `job_host` and `resource_host` to the same `addr`
    pub fn host<A: AsRef<str>>(self, addr: A) -> Self {
        let addr = addr.as_ref().to_string();
        self.job_host(&addr).resource_host(&addr)
    }
    /// Sets both `job_port` and `resource_port` to the same `port`
    pub fn port(self, port: u16) -> Self {
        self.job_port(port).resource_port(port)
    }
    pub fn client_tls_config(mut self, config: ClientTlsConfig) -> Self {
        self.client_tls_config = Some(config);
        self
    }
    pub fn disable_tls(mut self) -> Self {
        self.disable_tls = true;
        self
    }
    pub fn authenticator(mut self, authenticator: Authenticator) -> Self {
        self.authenticator = Some(authenticator);
        self
    }
    pub async fn build(self) -> Result<Client, BuilderError> {
        let scheme = if self.disable_tls { "http" } else { "https" };

        let client_config = self.client_tls_config.unwrap_or(ClientTlsConfig::new());

        let job_endpoint_uri = format!(
            "{}://{}:{}",
            scheme,
            self.job_host.unwrap_or("127.0.0.1".to_string()),
            self.job_port.unwrap_or(DEFAULT_JOB_PORT)
        );

        let mut job_endpoint: Endpoint = job_endpoint_uri
            .try_into()
            .map_err(|e: InvalidUri| BuilderError::Invalid("job endpoint uri", e.to_string()))?;

        if !self.disable_tls {
            job_endpoint = job_endpoint.tls_config(client_config.clone());
        }

        let job = JobServiceClient::connect(job_endpoint)
            .await
            .map_err(|e| BuilderError::Tonic(e))?;

        let resource_endpoint_uri = format!(
            "{}://{}:{}",
            scheme,
            self.resource_host.unwrap_or("127.0.0.1".to_string()),
            self.resource_port.unwrap_or(DEFAULT_RESOURCE_PORT)
        );

        let mut resource_endpoint: Endpoint =
            resource_endpoint_uri.try_into().map_err(|e: InvalidUri| {
                BuilderError::Invalid("resource endpoint uri", e.to_string())
            })?;

        if !self.disable_tls {
            resource_endpoint = resource_endpoint.tls_config(client_config.clone());
        }

        let resource = ResourceServiceClient::connect(resource_endpoint)
            .await
            .map_err(|e| BuilderError::Tonic(e))?;

        let authenticator = self.authenticator;

        Ok(Client {
            job,
            resource,
            authenticator,
        })
    }
}

pub struct Client {
    pub job: JobServiceClient<Channel>,
    pub resource: ResourceServiceClient<Channel>,
    authenticator: Option<Authenticator>,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }
}

macro_rules! impl_client {
    {
        $client:ident {
            $($meth:ident: $req:path, $resp:path,)*
        }
    } => {
        $(
            pub async fn $meth<R>(
                &mut self,
                request: R
            ) -> Result<Response<$resp>, Status>
            where
                R: IntoRequest<$req>
            {
                let mut request = request.into_request();
                if let Some(authenticator) = self.authenticator.as_ref() {
                    request = authenticator.authorize(request).await?;
                }
                self.$client.$meth(request).await
            }
        )*
    };
}

impl Client {
    impl_client! {
        job {
            get_job: GetJobRequest, GetJobResponse,
            insert_job: InsertJobRequest, InsertJobResponse,
            cancel_job: CancelJobRequest, CancelJobResponse,
            get_job_output_schema: GetJobOutputSchemaRequest, GetJobOutputSchemaResponse,
            get_job_output_rows: GetJobOutputRowsRequest, Streaming<GetJobOutputRowsResponse>,
        }
    }

    impl_client! {
        resource {
            acquire_lock: AcquireLockRequest, AcquireLockResponse,
            release_lock: ReleaseLockRequest, ReleaseLockResponse,
            list_resources: ListResourcesRequest, ListResourcesResponse,
            create_resource: CreateResourceRequest, CreateResourceResponse,
            update_resource: UpdateResourceRequest, UpdateResourceResponse,
            delete_resource: DeleteResourceRequest, DeleteResourceResponse,
        }
    }
}
