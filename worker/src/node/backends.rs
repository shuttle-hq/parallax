use crate::common::*;

use super::SharedState;
use crate::backends::{self, Backend as BackendTrait};
use crate::node::resource::BlockStore;
use crate::Result;

fn exec_from_resource(r: Resource, store: Store) -> Result<Arc<dyn BackendTrait>> {
    let t = r.block_type().map(|t| t.to_string())?;

    let backend = r.try_downcast::<Backend>().and_then(|v| v.try_unwrap())?;

    match backend {
        #[cfg(feature = "google-bigquery")]
        backend::Backend::BigQuery(big_query_backend) => {
            let bq = backends::BigQuery::new(big_query_backend)?;
            Ok(Arc::new(bq) as Arc<dyn BackendTrait>)
        }
        _ => {
            let error = Error::new_detailed(
                "backend type not supported",
                NotSupportedError {
                    feature: t.to_string(),
                },
            );
            Err(error)
        }
    }
}

#[derive(Clone)]
pub struct Backends {
    state: SharedState,
    stash: Arc<Mutex<HashMap<BlockType, Arc<dyn BackendTrait>>>>,
}

impl Backends {
    pub fn new(state: SharedState) -> Self {
        Self {
            state,
            stash: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub fn backend(&self, rt: &BlockType) -> Result<Arc<dyn BackendTrait>> {
        let mut stash = self.stash.lock().unwrap();

        if let Some(exec) = stash.get(rt) {
            Ok(exec.clone())
        } else {
            let store = self.state.store();
            let cached_backend = self
                .state
                .0
                .block(rt)?
                .map(move |r| exec_from_resource(r, store.clone()))
                .into_cached()
                .map_err(|e| ScopeError::from(e))?;
            let as_lazy =
                backends::LazyBackend::new(cached_backend, rt.clone(), self.state.store().clone());
            let exec = Arc::new(as_lazy);
            stash.insert(rt.clone(), exec.clone());
            Ok(exec)
        }
    }
}
