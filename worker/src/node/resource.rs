use serde::{Deserialize, Serialize};

pub use tonic::Status;

use std::collections::HashMap;
use std::iter::IntoIterator;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::vec::IntoIter;

use parallax_api::{
    jac::prelude::*,
    swamp::block::{Block, BlockType},
    TryUnwrap,
};

use derive_more::From;

use regex::Regex;

use crate::common::*;

pub trait BlockStore: Sized {
    type Error;

    type Item: jac::Read;

    fn block(&self, ty: &BlockType) -> Result<Self::Item, Self::Error>;

    fn ls(&self) -> Result<IntoIter<BlockType>, Self::Error>;

    fn into_shared(self) -> SharedBlockStore<Self> {
        SharedBlockStore {
            store: Arc::new(self),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

pub trait BlockStoreExt
where
    Self: BlockStore,
    <Self as BlockStore>::Error: From<<<Self as BlockStore>::Item as jac::Validate>::Error>,
{
    fn read<F, O>(&self, ty: &BlockType, f: F) -> Result<Option<O>, Self::Error>
    where
        F: FnOnce(&<Self::Item as jac::Validate>::Item) -> O,
    {
        self.block(ty)?
            .read_with(|block| f(block))
            .map_err(|e| e.into())
    }

    fn read_and_then<F, O>(&self, ty: &BlockType, f: F) -> Result<Option<O>, Self::Error>
    where
        F: FnOnce(&<Self::Item as jac::Validate>::Item) -> Result<O, Self::Error>,
    {
        self.read(ty, f).and_then(|inner| inner.transpose())
    }

    fn read_all<F, O>(&self, pat: &BlockType, mut f: F) -> Result<IntoIter<O>, Self::Error>
    where
        F: FnMut(&<Self::Item as jac::Validate>::Item) -> O,
    {
        let res = self
            .ls_match(pat)?
            .filter_map(|ty| self.read(&ty, |block| f(block)).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(res.into_iter())
    }

    fn ls_match(&self, pat: &BlockType) -> Result<IntoIter<BlockType>, Self::Error> {
        let buf: Vec<_> = self.ls()?.filter(|bt| pat.matches(&bt)).collect();
        Ok(buf.into_iter())
    }
}

impl<T> BlockStoreExt for T
where
    Self: BlockStore,
    <Self as BlockStore>::Error: From<<<Self as BlockStore>::Item as jac::Validate>::Error>,
{
}

pub struct RedisBlockStore<R> {
    prefix: Option<String>,
    store: jac::redis::Store,
    _item: std::marker::PhantomData<R>,
}

impl<R> RedisBlockStore<R> {
    pub fn from(store: Store) -> Self {
        Self {
            prefix: None,
            store,
            _item: std::marker::PhantomData,
        }
    }

    pub fn with_prefix(store: Store, prefix: &str) -> Self {
        let mut store = Self::from(store);
        store.prefix = Some(prefix.to_owned());
        store
    }
}

/// A block type with an optional prefix to disambiguate scopes
/// stored in the same backend
#[derive(Clone)]
pub struct StoreBlockType {
    prefix: Option<String>,
    ty: BlockType,
}

impl std::fmt::Display for StoreBlockType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix = self
            .prefix
            .as_ref()
            .map(|p| format!("{}/", p))
            .unwrap_or("".to_string());
        write!(f, "{}{}", prefix, self.ty)
    }
}

impl std::str::FromStr for StoreBlockType {
    type Err = swamp::TypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let outer: Vec<&str> = s.split("/").collect();
        let prefix;
        let ty;
        if outer.len() == 1 {
            prefix = None;
            ty = outer[0].parse()?;
        } else if outer.len() == 2 {
            prefix = Some(outer[0].to_owned());
            ty = outer[1].parse()?;
        } else {
            return Err(swamp::TypeError::Invalid(s.to_string()));
        }
        Ok(Self { prefix, ty })
    }
}

impl redis::ToRedisArgs for StoreBlockType {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        out.write_arg(self.to_string().as_bytes())
    }
}

impl<R> RedisBlockStore<R> {
    fn wrap_type(&self, ty: &BlockType) -> StoreBlockType {
        StoreBlockType {
            prefix: self.prefix.clone(),
            ty: ty.clone(),
        }
    }
}

impl<R> BlockStore for RedisBlockStore<R>
where
    for<'a> R: Deserialize<'a> + Serialize,
{
    type Error = ScopeError;
    type Item = jac::redis::CachedEntry<StoreBlockType, R>;
    fn block(&self, ty: &BlockType) -> Result<Self::Item, Self::Error> {
        let wrapped = self.wrap_type(ty);
        self.store
            .entry(wrapped)
            .and_then(|e| e.into_cached())
            .map_err(|e| e.into())
    }

    fn ls(&self) -> Result<IntoIter<BlockType>, Self::Error> {
        let pat = format!(
            "{}*",
            self.prefix
                .as_ref()
                .map(|p| format!("{}/", p))
                .unwrap_or_default()
        );
        let buf: Vec<BlockType> = self
            .store
            .list(&pat)?
            .map(|k: String| {
                let description =
                    "an invalid block type was encountered in the store, this should not happen"
                        .to_string();
                StoreBlockType::from_str(&k)
                    .map(|sbt| sbt.ty)
                    .map_err(|_| ScopeError {
                        source: k,
                        description,
                        ..Default::default()
                    })
            })
            .collect::<Result<_, _>>()?;
        Ok(buf.into_iter())
    }
}

pub struct SharedBlockStore<S: BlockStore> {
    store: Arc<S>,
    cache: Arc<Mutex<HashMap<BlockType, Arc<S::Item>>>>,
}

impl<S> SharedBlockStore<S>
where
    S: BlockStore,
{
    pub fn from(store: S) -> Self {
        store.into_shared()
    }
}

impl<S> std::clone::Clone for SharedBlockStore<S>
where
    S: BlockStore,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl<S> BlockStore for SharedBlockStore<S>
where
    S: BlockStore,
{
    type Error = S::Error;
    type Item = Arc<S::Item>;
    fn block(&self, ty: &BlockType) -> Result<Self::Item, Self::Error> {
        let mut cache = self.cache.lock().unwrap();
        if let Some(block) = cache.get(ty) {
            Ok(block.clone())
        } else {
            let block = Arc::new(self.store.block(ty)?);
            cache.insert(ty.clone(), block.clone());
            Ok(block)
        }
    }

    fn ls(&self) -> Result<IntoIter<BlockType>, Self::Error> {
        self.store.ls()
    }
}

/// A cluster-wide scope for blocks of type `R`
pub type SharedScope<R> = SharedBlockStore<RedisBlockStore<R>>;

/// A block of type `R` stored in cluster-wide memory
pub type Shared<R> = <SharedScope<R> as BlockStore>::Item;

#[derive(Debug)]
pub struct LocalBlockStore<R>(HashMap<BlockType, jac::Constant<R>>);

impl<R> LocalBlockStore<R>
where
    R: Block + Clone,
{
    pub fn from_iter<I>(iter: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = R>,
    {
        let mut inner = HashMap::new();
        for block in iter.into_iter() {
            let ty = block.block_type()?;
            if inner.contains_key(&ty) {
                let scope_err = ScopeError {
                    kind: ScopeErrorKind::AlreadyExists as i32,
                    source: ty.to_string(),
                    description: "block declared more than once".to_string(),
                };
                return Err(scope_err.into());
            } else {
                inner.insert(ty, jac::Constant::from_value(block));
            }
        }
        Ok(Self(inner))
    }
}

impl<R> BlockStore for LocalBlockStore<R>
where
    R: Clone,
{
    type Error = ScopeError;

    type Item = jac::Constant<R>;

    fn block(&self, ty: &BlockType) -> Result<Self::Item, Self::Error> {
        if let Some(existing) = self.0.get(ty) {
            Ok(existing.clone())
        } else {
            Err(ScopeError {
                kind: ScopeErrorKind::NotFound as i32,
                source: ty.to_string(),
                ..Default::default()
            })
        }
    }

    fn ls(&self) -> Result<IntoIter<BlockType>, Self::Error> {
        Ok(self.0.keys().cloned().collect::<Vec<_>>().into_iter())
    }
}

impl<R> std::default::Default for LocalBlockStore<R> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<R> LocalBlockStore<R>
where
    R: Block,
{
    pub fn get(&self, ty: &BlockType) -> Option<&R> {
        self.0.get(ty).and_then(|c| c.read())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::node::state::State;

    use std::fs::File;
    use std::io::Read as IoRead;

    fn read_manifest() -> State {
        let mut manifest = File::open("test/manifest.yaml").unwrap();
        let mut buf = String::new();
        manifest.read_to_string(&mut buf).unwrap();
        serde_yaml::from_str(&buf).unwrap()
    }

    fn state_as_scope(state: State) -> LocalBlockStore<Resource> {
        LocalBlockStore::from_iter(state.into_iter()).unwrap()
    }

    #[test]
    fn block_type_parse() {
        let ty = "resource.backend.big_query.my_big_query.data.table.my_table";
        let (consumed_res, left_res) = BlockType::parse::<Resource>(ty).unwrap();
        let (consumed_data, left_data) = BlockType::parse_stream::<Data>(left_res).unwrap();
        assert_eq!(
            consumed_res,
            block_type!("resource"."backend"."big_query"."my_big_query")
        );
        assert_eq!(consumed_data, block_type!("data"."table"."my_table"));
    }

    #[test]
    fn manifest_deserializes() {
        read_manifest();
    }

    #[test]
    fn manifest_parses_to_scope() {
        let state = read_manifest();
        state_as_scope(state);
    }

    fn store<R>() -> RedisBlockStore<R> {
        let random_prefix = uuid::Uuid::new_v4().to_simple().to_string();
        RedisBlockStore::with_prefix(Store::default(), &random_prefix)
    }

    #[test]
    fn manifest_to_store() {
        let state = read_manifest();
        let store = store();
        for resource in state.clone().into_iter() {
            let block = store.block(&resource.block_type().unwrap()).unwrap();
            let mut lock = block.write().unwrap();
            *lock = Some(resource);
            block.push(lock).unwrap();
        }

        for resource in state.into_iter() {
            let ty = resource.block_type().unwrap();
            let block = store.block(&ty).unwrap();
            block
                .read_with(|r| {
                    assert_eq!(r, &resource);
                })
                .unwrap()
                .unwrap();
        }
    }

    #[test]
    fn manifest_to_shared_store() {
        let state = read_manifest();
        let store = store().into_shared();
        for resource in state.clone().into_iter() {
            let block = store.block(&resource.block_type().unwrap()).unwrap();
            let mut lock = block.write().unwrap();
            *lock = Some(resource);
            block.push(lock).unwrap();
        }

        let scope = state_as_scope(state);

        store
            .read_all(&block_type!("*"), |block| {
                let ty = block.block_type().unwrap();
                let from_scope = scope.get(&ty).unwrap();
                assert_eq!(block, from_scope);
            })
            .unwrap();
    }
}
