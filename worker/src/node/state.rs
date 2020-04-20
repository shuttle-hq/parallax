use crate::common::*;
use crate::node::resource::{BlockStore, BlockStoreExt, RedisBlockStore, Shared, SharedScope};
use crate::opt::{Context, ContextKey, DataType, ExprMeta, Policy, PolicyBinding, TableMeta};

pub type Scope<R> = HashMap<BlockType, R>;

pub type State = Vec<Resource>;

pub struct SharedState(pub(super) SharedScope<Resource>, Store);

impl Clone for SharedState {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}

impl SharedState {
    pub(super) fn new(store: Store, scope: &str) -> Self {
        Self(
            RedisBlockStore::with_prefix(store.clone(), scope).into_shared(),
            store.clone(),
        )
    }

    pub fn store(&self) -> Store {
        self.1.clone()
    }

    pub fn resource(&self, resource_ty: &BlockType) -> Result<Shared<Resource>, ScopeError> {
        self.0.block(resource_ty)
    }

    pub fn resources(&self, pat: &BlockType) -> Result<Vec<Resource>, ScopeError> {
        debug!("getting all resources with {}", pat);
        let res = self.0.read_all(pat, |block| block.clone())?;
        Ok(res.collect())
    }

    pub fn acquire_lock(&self) -> Result<String, ScopeError> {
        unimplemented!();
    }

    pub fn release_lock(&self, lock_id: &str) -> Result<(), ScopeError> {
        unimplemented!();
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use std::fs::File;
    use std::io::Read;

    pub fn read_manifest() -> State {
        let mut manifest = File::open("test/manifest.yaml").unwrap();
        let mut buf = String::new();
        manifest.read_to_string(&mut buf).unwrap();
        serde_yaml::from_str(&buf).unwrap()
    }

    pub fn mk_state(store: Store, scope: &str) -> SharedState {
        SharedState::new(store, scope)
    }
}
