use crate::common::*;
use crate::Result;

use super::{BlockStore, BlockStoreExt, RedisBlockStore, SharedScope};
use crate::job::Job;
use std::net::IpAddr;

/// Location of a node on the cluster
#[derive(Serialize, Deserialize, Clone)]
pub struct Peer {
    pub id: String,
    pub addr: IpAddr,
    pub port: u16,
}

impl Default for Peer {
    fn default() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_simple().to_string(),
            addr: "127.0.0.1".parse().unwrap(),
            port: 20000, // FIXME
        }
    }
}

pub struct Cluster {
    /// Who we advertise as in the cluster
    pub advertised: Peer,
    /// The other (including us) peers in the pool
    pub peers: SharedScope<Peer>,
    pub jobs: SharedScope<Job>,
}

impl Block for Peer {
    fn block_type(&self) -> std::result::Result<BlockType, parallax_api::swamp::TypeError> {
        Ok(block_type!("peer".(&self.id)))
    }

    fn parse_block_type(
        stream: TokenStream,
    ) -> std::result::Result<(BlockType, TokenStream), parallax_api::swamp::TypeError> {
        stream.parse("peer")?.take()?.done()
    }
}

impl Cluster {
    pub fn join_as(store: Store, advertised: Peer) -> Result<Self> {
        info!("joining pool as {}", advertised.addr);

        let peers = RedisBlockStore::with_prefix(store.clone(), "cluster").into_shared();

        let advertised_ty = advertised.block_type()?;

        let entry = peers.block(&advertised_ty)?;

        let mut lock = entry.write()?;
        *lock = Some(advertised.clone());
        if let Err(WriteError { lock, .. }) = entry.push(lock) {
            unimplemented!("recover from poison")
        }

        let jobs = RedisBlockStore::with_prefix(store, "jobs").into_shared();

        Ok(Self {
            advertised,
            peers,
            jobs,
        })
    }

    fn release(&self) -> Result<()> {
        info!("releasing {} (self) to the pool", self.advertised.addr);

        let self_ty = self.advertised.block_type()?;
        let entry = self.peers.block(&self_ty)?;
        let mut lock = entry.write()?;
        *lock = None;
        if let Err(WriteError { lock, .. }) = entry.push(lock) {
            unimplemented!("recover from poison")
        }

        Ok(())
    }

    pub fn leave(mut self) -> Result<()> {
        self.release()
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.release().expect("could not release self to pool");
    }
}
