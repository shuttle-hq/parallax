#[macro_use]
extern crate async_trait;

pub extern crate swamp;
pub use swamp::{block_type, Block, BlockType, Label, TokenStream, TypeError};

pub use tonic;

pub use jac::{self, prelude::*};

pub mod proto;

#[cfg(feature = "client")]
pub mod client;

pub use proto::parallax::service::{
    job::v1::{
        job_service_client::JobServiceClient,
        job_service_server::{JobService, JobServiceServer},
        job_status::JobState,
        *,
    },
    resource::v1::{
        resource_service_client::ResourceServiceClient,
        resource_service_server::{ResourceService, ResourceServiceServer},
        *,
    },
};

pub use proto::parallax::config::resource::v1::{
    backend::Backend as BackendEnum, data::Data as DataEnum, policy::Policy as PolicyEnum,
    resource::Resource as ResourceEnum, *,
};

pub use proto::parallax::r#type::error::v1::{
    access_error::AccessErrorKind, backend_error::BackendErrorKind, error::Details,
    scope_error::ScopeErrorKind, *,
};

pub trait TryUnwrap {
    type Into;
    fn try_unwrap(self) -> std::result::Result<Self::Into, ScopeError>;
}
