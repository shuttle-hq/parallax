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

        let state = SharedState::new(store.clone(), "gobal");

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
    } // FIXME
    fn default_group(&self) -> &str {
        "root"
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

    const SAMPLE_SECRET: &'static str = "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQD+xoWJzXNAYEvXipD5xxQDx8pb6vDDX1afgcF86gTYkjclQqTXjfkUTPyESWdCyPYI1joQfCFq06nGZlUOZjIEpDVkYvE+KOOI/ojh2GwLXZlspQ54/qi6AU3YlcT1tYF6jrL/TOvsZOo0cCP5TZHrOHknfpnZD2vYIs64AuJ+mzkt0UPTz7P3Z0E8GmuUVvJQ7k0ZzjslZMdw6aBpSrQcTg480azwzwkFRdq6tWcZhn1wK4SagkU/h0L4vYkNwsLcZsEmcLnKdHSsN7mtN4xKpC8vcZk6Th8DN1g+YkV9yQKcuLr6xhCWahw7RFk/2gQHvlLyPPFziC9FnTaK6zIhAgMBAAECggEBAIQg6LhiuFa2mh6aWj1vpU2gm6231k08cGwgmvvxUboPelH0khDILFj+5Cam+sqD9jT3BP3volUImI+wGoRxM5d8ocQCHCKkifxOJScbWk06qYrSvwK470An1PtyEMds7k1lHCeS+PeMDnpLfhyYwgg/LXXyDk+n08Ivqw17UYNebA+/2xNs2WTMnjTPZsCgBCrsYIJcjf3xAnG9I6sC8nMI9KtTcwa6JlneAvEazk0XlDL6iu5fOWIB9fazw3W/c9eCGou1yL/eeDtXd5a/iOBQok1OxPNBWgIE1SB12y4xJ9WS3H2pi37qEhCn0b4kAQ69/p2AWHrNZc1Yzl9E9BECgYEA/2o4e8D+Hvjt9LeNC44SC97gkNQCRUR6M6brKkuIu4/usmAauHVIipfQiINvmcCKHfPwPyzQemmxnILB1LbLqcYiBGuscLLYXWBrJ1/q7VXAz3MONYPVP2qYodfnTNUnx42myZq9RNSmzdvsAmsaIruUe6UKzW3+OR3lhxDeM78CgYEA/1vtDybLVttrYxptpz9fGxZt8HbeIAoUPecZ3hZ6IlJCcBszJ/xI503SI+pXjVscXAgJRiUJ+spM5e+oiLuSXf+JgS6yqDHbeuqq9NW6DLcNtvFQyIlxc81K/hDVVChucwRDjHEC6YUpw9op9CVrfy3cjUAB66h1YR64OtmAkh8CgYEAkuNJnJI/Exzh7IzrBqwr4LvKtj/XFnLtPGtQb0CFYCjEg70VYOPCVkI84WWFdwzt4Y/6qLpjIyNJy9K/GlAODJMv2Q77Wszf9cOPnFNjTh61BhkLIOeyaggpw1nhYaRsfi0OsH7XPpB1ZYqGzlU98J+kIv+lmMMbI0n4SJcA0w0CgYEAg4f5Im3MHTAmL0TkqFonBc/Wzx6bKYung38vPsr7HASc/hu0jOsRLjtSe3dwo3oDsI/fHwdjDY/4bhO1DsD3En3WePjxw1ry/2wypKwWCgicAthn0POiwZBECXvoNlIhGhNfK87mPhx+N8h/Bafkp/yadxM6KgV6eI0XPhDcfecCgYAVTIPMemxfjv/gDgZINhjbnrXHctIuysZqU7emnikxHkHFcqfDlbI2C3QtXKkXw3X+Ba+YxaEoHm+JSIVEWR30Km/5AQr+KIAdfMCk9ErxkhsuFb8duZ7jPIJ93ChC3gj+Ve3xWGe1eCI4mELoHXYFvRGTMl0+OUzxBiQfHcc5EA==";

    pub fn mk_key_pair() -> RsaKeyPair {
        let secret_dat = base64::decode(SAMPLE_SECRET).unwrap();
        RsaKeyPair::from_pkcs8(&secret_dat).unwrap()
    }

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
