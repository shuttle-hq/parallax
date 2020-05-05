use crate::common::*;
use crate::Result;

pub use super::{Backends, Node, Peer, Scope, Shared};

use crate::backends::Backend;
use crate::job::{Job, Processor};
use crate::opt::PolicyBinding;
use crate::opt::{Context, TableMeta};

macro_rules! access_error {
    ($culprit:ident: $kind:ident, $desc:tt $(, $arg:tt)*) => {
        {
            let mut err = access_error!($kind, $desc $(, $arg)*);
            err.culprit = $culprit.user.to_string();
            debug!("access error: {}", err);
            err
        }
    };
    ($kind:ident, $desc:tt $(, $arg:tt)*) => {
        AccessError {
            kind: AccessErrorKind::$kind as i32,
            description: format!($desc $(, $arg)*),
            ..Default::default()
        }
    };
    ($culprit:ident: $kind:ident) => {
        {
            let mut err = access_error!($kind);
            err.culprit = $culprit.user.to_string();
            debug!("access error: {}", err);
            err
        }
    };
    ($kind:ident) => {
        AccessError {
            kind: AccessErrorKind::$kind as i32,
            ..Default::default()
        }
    };
}

mod account;
pub use account::{AccountAccess, AccountAccessProvider};

mod bootstrap;
pub use bootstrap::BootstrapAccessProvider;

use super::ops;

lazy_static! {
    static ref TOKEN_REG: regex::Regex = { regex::Regex::new("Bearer (.+)").unwrap() };
}

fn get_token_for_req<T>(req: &Request<T>) -> AccessResult<&str> {
    let header = req
        .metadata()
        .get("Authorization")
        .ok_or(access_error!(BadRequest, "authorization missing"))?
        .to_str()
        .map_err(|_| access_error!(BadRequest, "illegal authorization header"))?;
    Ok(TOKEN_REG
        .captures(header)
        .ok_or(access_error!(
            BadRequest,
            "authorization not a bearer token"
        ))?
        .get(1)
        .ok_or(access_error!(BadRequest, "authorization no token found"))?
        .as_str())
}

pub type AccessResult<T> = std::result::Result<T, AccessError>;

pub trait AccessProvider: Clone + Send + Sync {
    type Access: Access;
    fn elevate<R>(&self, req: &Request<R>) -> AccessResult<Self::Access>;
}

#[tonic::async_trait]
pub trait Access: Sized + Send + Sync + 'static {
    fn who_am_i(&self) -> &str;
    fn default_group(&self) -> &str;
    fn shared_job(&self, job_id: &str) -> Result<Shared<Job>>;
    fn backend(&self, resource_type: &BlockType) -> Result<Arc<dyn Backend>>;
    fn peer(&self) -> Result<Peer>;
    fn resource(&self, resource_ty: &BlockType) -> Result<Shared<Resource>>;
    fn resources(&self, pat: &BlockType) -> Result<Vec<Resource>>;
    fn acquire_lock(&self) -> Result<String>;
    fn release_lock(&self, lock_id: &str) -> Result<()>;
    fn list_jobs(&self) -> Result<Vec<Job>>;

    fn job(&self, job_id: &str) -> Result<Option<Job>> {
        self.shared_job(job_id).and_then(|job| {
            job.clone_inner()
                .map_err(|e| ScopeError::from(e))
                .map_err(|e| Error::from(e))
        })
    }

    fn policies_for_group(&self, audience: &str) -> Result<Context<PolicyBinding>> {
        ops::policies_for_group(self, audience)
    }

    async fn context(&self) -> Result<Context<TableMeta>> {
        ops::context(self).await
    }

    fn into_task(self, task_id: &str) -> Processor<Self> {
        Processor::new(self, task_id.to_string())
    }

    fn into_new_task(self) -> Processor<Self> {
        self.into_task(&Uuid::new_v4().to_simple().to_string())
    }

    fn user(&self, user_id: &str) -> Result<Option<User>> {
        ops::user(self, user_id)
    }

    fn group(&self, group_id: &str) -> Result<Option<Group>> {
        ops::group(self, group_id)
    }

    fn groups_for_user(&self, user_id: &str) -> Result<Vec<BlockType>> {
        ops::groups_for_user(self, user_id)
    }

    fn create_resource(&self, resource: Resource) -> Result<Resource> {
        ops::create_resource(self, resource)
    }

    fn update_resource(&self, resource_ty: &BlockType, resource: Resource) -> Result<Resource> {
        ops::update_resource(self, resource_ty, resource)
    }

    fn delete_resource(&self, resource_ty: &BlockType) -> Result<()> {
        ops::delete_resource(self, resource_ty)
    }

    fn error<E: Into<Error>>(&self, err: E) -> AccessError {
        AccessError {
            kind: AccessErrorKind::Unknown as i32,
            culprit: self.who_am_i().to_string(),
            cause: Some(Box::new(err.into())),
            ..Default::default()
        }
    }

    fn forbidden(&self) -> AccessError {
        AccessError {
            kind: AccessErrorKind::Forbidden as i32,
            culprit: self.who_am_i().to_string(),
            ..Default::default()
        }
    }
}
