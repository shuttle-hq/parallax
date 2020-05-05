use std::{error, fmt};

use std::collections::HashMap;
use std::convert::{From, TryFrom, TryInto};
use std::fs::File;
use std::io::Read;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use derive_more::From;

use crate::common::*;
use crate::Result;

use crate::backends::Backend;
use crate::job::Job;
use crate::opt::validate::Validate;
use crate::opt::{Context, ContextKey, DataType, ExprMeta, PolicyBinding, TableMeta};
use crate::Opt;

pub mod access;

pub use access::{Access, AccessProvider, AccessResult};

#[cfg(not(test))]
mod state;
#[cfg(test)]
pub mod state;

pub use state::{Scope, SharedState};

mod backends;

pub use backends::Backends;

mod resource;

pub use resource::{
    BlockStore, BlockStoreExt, RedisBlockStore, Shared, SharedBlockStore, SharedScope,
};

mod cluster;

pub use cluster::{Cluster, Peer};

pub(self) mod ops;

pub struct Node {
    cluster: Cluster,
    pub state: SharedState,
    backends: Backends,
}

impl Node {
    pub async fn new(opt: &Opt) -> Result<Self> {
        let advertised_host = opt
            .advertised_host
            .clone()
            .or_else(|| {
                ifaces::Interface::get_all()
                    .ok()
                    .and_then(|mut ifs| ifs.pop())
                    .and_then(|if_| if_.addr)
                    .map(|addr| addr.ip())
            })
            .expect("could not find an IP address to advertise");

        let advertised = Peer {
            addr: advertised_host,
            port: opt.cluster_port,
            ..Default::default()
        };

        let redis_url = format!("redis://{}:{}", opt.redis_host, opt.redis_port);
        info!("redis_url={}", redis_url);

        let redis_password = (&opt.redis_password_file)
            .as_ref()
            .and_then(|password_file| {
                let mut password = String::new();
                File::open(password_file)
                    .ok()?
                    .read_to_string(&mut password)
                    .expect("IO");
                info!("found a redis password");
                Some(password)
            });

        let mut builder = Store::builder().url(&redis_url);

        if let Some(password) = redis_password {
            builder = builder.password(&password);
        }

        let store = builder.build().unwrap();

        let cluster = Cluster::join_as(store.clone(), advertised)?;

        let state = SharedState::new(store.clone(), "global");

        let backends = Backends::new(state.clone());

        Ok(Self {
            cluster,
            state,
            backends,
        })
    }
}

#[tonic::async_trait]
impl Access for Arc<Node> {
    fn who_am_i(&self) -> &str {
        "root"
    }
    // FIXME
    fn default_group(&self) -> &str {
        "root"
    }

    fn list_jobs(&self) -> Result<Vec<Job>> {
        // This is in the root context so it should list all jobs
        let jobs = self
            .cluster
            .jobs
            .read_all(&block_type!("job"."*"), |block| block.clone())?
            .collect();
        Ok(jobs)
    }

    fn shared_job(&self, job_id: &str) -> Result<Shared<Job>> {
        debug!("looking up job `{}`", job_id);
        self.cluster
            .jobs
            .block(&block_type!("job".job_id))
            .map_err(|e| e.into())
    }

    fn backend(&self, resource_type: &BlockType) -> Result<Arc<dyn Backend>> {
        self.backends.backend(resource_type)
    }

    fn peer(&self) -> Result<Peer> {
        Ok(self.cluster.advertised.clone())
    }

    fn acquire_lock(&self) -> Result<String> {
        self.state.acquire_lock().map_err(|e| e.into())
    }

    fn release_lock(&self, lock_id: &str) -> Result<()> {
        self.state.release_lock(lock_id).map_err(|e| e.into())
    }

    fn resource(&self, resource_ty: &BlockType) -> Result<Shared<Resource>> {
        self.state.resource(resource_ty).map_err(|e| e.into())
    }

    fn resources(&self, pat: &BlockType) -> Result<Vec<Resource>> {
        self.state.resources(pat).map_err(|e| e.into())
    }
}

pub async fn new(opt: &Opt) -> Result<Node> {
    Node::new(opt).await
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use super::state::tests::{mk_state, read_manifest};

    use ring::signature::{KeyPair, RsaKeyPair};

    use tokio::runtime::Runtime;

    pub fn mk_random_node() -> Node {
        let random = uuid::Uuid::new_v4().to_simple().to_string();
        mk_node(&random)
    }

    pub fn mk_node(scope: &str) -> Node {
        let store = Store::default();
        let state = mk_state(store.clone(), scope);
        let cluster = Cluster::join_as(store, Peer::default()).unwrap();
        let backends = Backends::new(state.clone());
        let node = Node {
            state,
            cluster,
            backends,
        };
        node
    }

    pub fn just_get_me_policies() -> Context<PolicyBinding> {
        let random_scope = uuid::Uuid::new_v4().to_simple().to_string();
        let access = Arc::new(mk_node(&random_scope));
        for resource in read_manifest().into_iter() {
            access.create_resource(resource).unwrap();
        }
        access.policies_for_group("group1").unwrap()
    }

    pub fn just_get_me_a_context() -> Context<TableMeta> {
        let random_scope = uuid::Uuid::new_v4().to_simple().to_string();
        let access = Arc::new(mk_node(&random_scope));
        for resource in read_manifest().into_iter() {
            access.create_resource(resource).unwrap();
        }
        Runtime::new()
            .unwrap()
            .block_on(async { access.context().await.unwrap() })
    }

    #[test]
    fn cluster_added_then_released_on_drop() {
        let peer = Peer::default();
        let peer_ty = peer.block_type().unwrap();

        let store = {
            let store = Store::default();
            let cluster = Cluster::join_as(store.clone(), peer.clone());
            let redis_store = RedisBlockStore::with_prefix(store, "cluster");

            let peer_id = redis_store
                .read(&peer_ty, |peer: &Peer| peer.id.clone())
                .unwrap()
                .expect("peer has not been added");

            assert_eq!(peer.id, peer_id);

            redis_store
        };

        let m_peer = store
            .read(&peer_ty, |peer: &Peer| {
                panic!("peer should have disappeared");
            })
            .unwrap();

        assert!(m_peer.is_none())
    }
}
