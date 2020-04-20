use biscuit::jwa::SignatureAlgorithm;
use biscuit::jwe::RegisteredHeader;
use biscuit::jwk::RSAKeyParameters;
use biscuit::jws::{self, Secret};
use biscuit::{RegisteredClaims, StringOrUri};
pub type JWT = biscuit::JWT<(), jws::Header<()>>;

use crate::common::*;
use crate::Result;

use super::{get_token_for_req, Access, AccessProvider, AccessResult};
use crate::backends::Backend;
use crate::job::{Job, Processor};
use crate::node::{Peer, Shared};
use crate::opt::PolicyBinding;
use crate::opt::{Context, TableMeta};

/// An AccessProvider giving access to users after direct
/// identity validation and only to the appropriate level
/// of privilege.
#[derive(Clone)]
pub struct AccountAccessProvider<A> {
    inner: A,
}

impl<A> AccountAccessProvider<A>
where
    A: Access + Clone,
{
    pub fn new(inner: A) -> Self {
        Self { inner }
    }
}

/// The Access provided by the AccountProvider.
#[derive(Clone)]
pub struct AccountAccess<A> {
    user: BlockType,
    primary_group: BlockType,
    super_user: bool,
    inner: A,
}

impl<A> AccessProvider for AccountAccessProvider<A>
where
    A: Access + Clone,
{
    type Access = AccountAccess<A>;
    fn elevate<R>(&self, req: &Request<R>) -> AccessResult<Self::Access> {
        let token = get_token_for_req(req)?;
        debug!("AccountProvider: elevating a claim '{}'", token);
        let mut it = token.split(".");

        // drop the first chunk, which is headers
        it.next();

        let claims_str = it
            .next()
            .ok_or(access_error!(BadRequest, "no claims in bearer token"))?;

        let claims_dec = base64::decode(claims_str)
            .map_err(|_| access_error!(BadRequest, "claims not valid base64"))?;

        let claims: RegisteredClaims = serde_json::from_slice(claims_dec.as_slice())
            .map_err(|_| access_error!(BadRequest, "invalid JSON claims set"))?;

        let subject = claims
            .subject
            .ok_or(access_error!(BadRequest, "claim subject cannot be null"))?;

        let user_id = match subject {
            StringOrUri::String(sub) => Ok(sub),
            _ => Err(access_error!(BadRequest, "claim subject cannot be a uri")),
        }?;

        let User {
            public_keys,
            primary_group,
            super_user,
            ..
        } = self
            .inner
            .user(&user_id)
            .map_err(|_| access_error!(Unavailable, "could not look for user"))?
            .ok_or(access_error!(NotFound, "user not found"))?;

        let primary_group = BlockType::parse::<Resource>(&primary_group)
            .map_err(|e| access_error!(Unavailable, "invalid primary group {}", e))?
            .0;

        let user = block_type!("resource"."user".(&user_id));

        let jwt = JWT::new_encoded(token);

        let mut keyring = public_keys.into_iter().filter_map(|key_base64| {
            let key_jwk = base64::decode(&key_base64).ok()?;
            let key_params: RSAKeyParameters = serde_json::from_slice(&key_jwk).ok()?;
            Some(Secret::from(key_params))
        });

        Ok(AccountAccess {
            user,
            primary_group,
            super_user,
            inner: self.inner.clone(),
        })
        /* BROKEN
        for key in keyring {
            match jwt.decode(&key, SignatureAlgorithm::RS256) {
                Ok(_) => return Ok(Access { user, primary_group, node: node.clone() }),
                Err(_) => continue
            }
        }

        Err(access_error!(NotFound, "no valid key for user_id `{}`", user_id))
         */
    }
}

impl<A> AccountAccess<A> {
    pub fn user_id(&self) -> &str {
        &self.user.labels()[1]
    }

    pub fn primary_group_id(&self) -> &str {
        &self.primary_group.labels()[1]
    }
}

impl<A> AccountAccess<A>
where
    A: Access,
{
    pub fn ensure_super(&self) -> Result<()> {
        if !self.super_user {
            return Err(self.forbidden().into());
        } else {
            Ok(())
        }
    }
}

#[tonic::async_trait]
impl<A> Access for AccountAccess<A>
where
    A: Access,
{
    fn who_am_i(&self) -> &str {
        self.user_id()
    }

    fn default_group(&self) -> &str {
        self.primary_group_id()
    }

    fn shared_job(&self, job_id: &str) -> Result<Shared<Job>> {
        self.inner.shared_job(job_id)
    }

    fn backend(&self, rt: &BlockType) -> Result<Arc<dyn Backend>> {
        self.inner.backend(rt)
    }

    fn resource(&self, resource_ty: &BlockType) -> Result<Shared<Resource>> {
        self.ensure_super()?;
        self.inner.resource(resource_ty)
    }

    fn resources(&self, pat: &BlockType) -> Result<Vec<Resource>> {
        self.inner.resources(pat)
    }

    fn peer(&self) -> Result<Peer> {
        self.inner.peer()
    }

    fn acquire_lock(&self) -> Result<String> {
        self.ensure_super()?;
        self.inner.acquire_lock()
    }

    fn release_lock(&self, lock_id: &str) -> Result<()> {
        self.ensure_super()?;
        self.inner.release_lock(lock_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::ops::create_resource;
    use crate::node::tests::mk_random_node;

    use crate::common::Block;
    use std::sync::Arc;

    #[test]
    fn access_account_enforce_super_user() {
        let node = Arc::new(mk_random_node());

        let user = User {
            name: "root".to_string(),
            super_user: true,
            ..Default::default()
        };

        let resource = Resource {
            resource: Some(user.into()),
        };

        create_resource(&node, resource.clone()).unwrap();

        let priv_access = AccountAccess {
            user: resource.block_type().unwrap(),
            primary_group: block_type!("resource"."group"."wheel"),
            super_user: true,
            inner: node.clone(),
        };

        priv_access
            .resource(&resource.block_type().unwrap())
            .expect("could not get resource");

        let unpriv_access = AccountAccess {
            user: block_type!("resource"."user"."lambda"),
            primary_group: block_type!("resource"."group"."users"),
            super_user: false,
            inner: node,
        };

        match unpriv_access.resource(&resource.block_type().unwrap()) {
            Err(_) => {}
            Ok(_) => panic!("should not have access to this"),
        }
    }
}
