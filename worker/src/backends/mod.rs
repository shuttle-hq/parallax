use serde::{Deserialize, Serialize};

use crate::common::*;
use crate::node::{BlockStore, RedisBlockStore, Shared, SharedScope, SharedState};
use crate::opt::{
    plan::Step, Context, ContextKey, ExprMeta, MaximumFrequency, RowCount, TableMeta,
};
use crate::Result;

use crate::opt::Domain;

#[cfg(feature = "google-bigquery")]
pub mod bigquery;

#[cfg(feature = "google-bigquery")]
pub use bigquery::BigQuery;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LastUpdated(Option<u64>);

impl From<u64> for LastUpdated {
    fn from(val: u64) -> Self {
        LastUpdated(Some(val))
    }
}

macro_rules! make_probe {
    {
        $vis:vis trait Probe {
            $(
                async fn $fn_ident:ident(
                    &self $(, $arg_name:ident:&$lifetime:lifetime$arg_ty:ty)*
                ) -> Result<$fn_output:ident>;
            )*
        }
    } => {
        #[async_trait]
        $vis trait Probe: Send + Sync {
            $(
                async fn $fn_ident(&self $(, $arg_name:&$arg_ty)*) -> Result<$fn_output>;
            )*
        }

        #[derive(Serialize, Deserialize, Clone, Debug)]
        $vis enum ProbeData {
            $($fn_output($fn_output),)*
        }
                /// Wrapper around ProbeData which also holds a time indicating when the cache
        /// was updated
        #[derive(Serialize, Deserialize, Clone, Debug)]
        struct CachedProbeData {
            inner: ProbeData,
            last_updated: LastUpdated
        }


        $(
            impl TryInto<$fn_output> for CachedProbeData {
                type Error = Error;
                fn try_into(self) -> Result<$fn_output> {
                    self.inner.try_into()
                }
            }

            impl TryInto<$fn_output> for ProbeData {
                type Error = Error;
                fn try_into(self) -> Result<$fn_output> {
                    match self {
                        Self::$fn_output(val) => Ok(val),
                        _ => Err(Error::new("incorrect data type".to_string()))
                    }
                }
            }

            impl Label for $fn_output {
                fn label(
                    &self,
                    builder: &mut BlockType
                ) -> std::result::Result<(), TypeError> {
                    let label = to_snake_case(stringify!($fn_output));
                    builder.push_label(&label);
                    Ok(())
                }
                fn parse_label(
                    s: TokenStream
                ) -> std::result::Result<TokenStream, TypeError> {
                    s.parse(&to_snake_case(stringify!($fn_output)))
                }
            }
        )*

        $vis struct LazyProbe<B: ?Sized> {
            inner: Arc<B>,
            key: ContextKey,
            cache: SharedScope<CachedProbeData>
        }

        impl Block for ProbeData {
            fn block_type(
                &self
            ) -> std::result::Result<BlockType, TypeError> {
                let mut bt = BlockType::build("probe_data");
                match self {
                    $(
                        Self::$fn_output(val) => val.label(&mut bt)?,
                    )*
                };
                Ok(bt)
            }
            fn parse_block_type(
                mut s: TokenStream
            ) -> std::result::Result<(BlockType, TokenStream), TypeError> {
                s = s.parse("probe_data")?;
                s = match s.peek()? {
                    $(
                        val if val == &to_snake_case(stringify!($fn_output)) => {
                            s.pass::<$fn_output>()
                        },
                    )*
                    other => Err(TypeError::Invalid(other.to_string()))
                }?;
                s.done()
            }
        }

        /// This does not use #[async_trait] because of compiler bug, see
        /// [#46](https://github.com/dtolnay/async-trait/issues/46)
        impl<B> Probe for LazyProbe<B>
        where
            B: Backend + ?Sized,
        {
            $(
                fn $fn_ident<'life0 $(, $lifetime)*, 'async_trait>(
                    &'life0 self $(, $arg_name:&$lifetime$arg_ty)*
                ) -> Pin<Box<dyn Future<Output = Result<$fn_output>> + Send + 'async_trait>>
                where
                    $($lifetime: 'async_trait,)*
                    'life0: 'async_trait,
                    Self: 'async_trait
                {
                    async move {
                        let ty = stringify!($fn_output).to_owned();
                        let ident = stringify!($fn_ident).to_owned();

                        let mut labels = vec![];
                        labels.push(ident);
                        $(labels.push($arg_name.to_string());)*

                        let block_type = BlockType::new(&ty, &labels);
                        let shared_cached_probe_data: Shared<CachedProbeData> = self.cache.block(&block_type)?;
                        let maybe_cached_probe_data: Option<CachedProbeData> = shared_cached_probe_data
                            .read_with(|inner| inner.clone())?;

                        match maybe_cached_probe_data {
                            None => {
                                debug!("Cache miss [{}]: <{}>", &self.key, &block_type);
                                let mut lock = shared_cached_probe_data.write()?;
                                let last_updated = self.inner
                                    .probe(&self.key)
                                    .await?
                                    .last_updated()
                                    .await?;
                                let $fn_ident = self.inner
                                    .probe(&self.key)
                                    .await?
                                    .$fn_ident($($arg_name,)*)
                                    .await?;
                                let probe_data = ProbeData::$fn_output($fn_ident.clone());
                                let cached_probe_data = CachedProbeData {
                                    inner: probe_data,
                                    last_updated,
                                };
                                *lock = Some(cached_probe_data);
                                if let Err(WriteError { lock, .. }) = shared_cached_probe_data.push(lock) {
                                    unimplemented!("recover from poison")
                                }
                                Ok($fn_ident)
                            }
                            Some(cached_probe_data) => {
                                debug!("Cache hit [{}]: <{}, {:?}>",&self.key, &block_type, cached_probe_data);
                                let last_updated = self.inner
                                    .probe(&self.key)
                                    .await?
                                    .last_updated()
                                    .await?;
                                if last_updated == cached_probe_data.last_updated {
                                    debug!("Returning cached!");
                                    return Ok(cached_probe_data.try_into()?);
                                }
                                let mut lock = shared_cached_probe_data.write()?;
                                let $fn_ident = self.inner
                                    .probe(&self.key)
                                    .await?
                                    .$fn_ident($($arg_name,)*)
                                    .await?;
                                let probe_data = ProbeData::$fn_output($fn_ident.clone());
                                let cached_probe_data = CachedProbeData {
                                    inner: probe_data,
                                    last_updated,
                                };
                                *lock = Some(cached_probe_data);
                                if let Err(WriteError { lock, .. }) = shared_cached_probe_data.push(lock) {
                                    unimplemented!("recover from poison")
                                }
                                Ok($fn_ident)
                            }
                        }
                    }
                    .boxed()
                }
            )*
        }
    };
}

make_probe! {
    pub trait Probe {
        async fn expr(&self, key: &'life1 ContextKey) -> Result<ExprMeta>;
        async fn to_meta(&self) -> Result<TableMeta>;
        async fn domain(&self, key: &'life1 ContextKey) -> Result<Domain>;
        async fn maximum_frequency(&self, key: &'life1 ContextKey) -> Result<MaximumFrequency>;
        async fn row_count(&self) -> Result<RowCount>;
        async fn last_updated(&self) -> Result<LastUpdated>;
    }
}

#[tonic::async_trait]
pub trait Backend: Send + Sync {
    async fn compute(&self, stage: Step) -> Result<()>;

    async fn probe<'a>(&'a self, key: &'a ContextKey) -> Result<Box<dyn Probe + 'a>>;

    /// Retrieve the Results from the remote backend
    async fn get_records(&self, data_id: &ContextKey) -> Result<ContentStream<ArrowRecordBatch>>;

    async fn get_schema(&self, data_id: &ContextKey) -> Result<ArrowSchema>;
}

pub struct LazyBackend<C, E: ?Sized> {
    from_cache: C,
    ty: BlockType,
    exec: PhantomData<E>,
    store: Store,
}

impl<C, E> LazyBackend<C, E>
where
    C: jac::Read<Item = Result<Arc<E>>, Error = jac::redis::Error> + Send + Sync,
    E: Backend + ?Sized,
{
    pub fn new(from_cache: C, ty: BlockType, store: Store) -> Self {
        Self {
            from_cache,
            ty,
            exec: PhantomData,
            store,
        }
    }
    fn to_inner(&self) -> Result<Arc<E>> {
        let inner = self
            .from_cache
            .read_with(|e| e.clone())
            .map_err(|e| ScopeError::from(e))?
            .ok_or(BackendError {
                kind: BackendErrorKind::Missing as i32,
                source: self.ty.to_string(),
                description: "backend resource has been removed".to_string(),
            })?;
        inner
    }
}

/// If state changes while a job is executing, the below code will first keep running
/// on the old state. If broken into steps, next time it gets called, it will be updated
/// and run on the new one. This could lead to unsafe inconsistencies in the result.
/// We probably want to enforce failing try_read_with if write lock has been acquired,
/// so that all running jobs will fail, which is less convenient to user but much safer.
#[tonic::async_trait]
impl<C, E> Backend for LazyBackend<C, E>
where
    C: jac::Read<Item = Result<Arc<E>>, Error = jac::redis::Error> + Send + Sync,
    E: Backend + ?Sized,
{
    async fn compute(&self, step: Step) -> Result<()> {
        self.to_inner()?.compute(step).await
    }
    async fn probe<'a>(&'a self, key: &'a ContextKey) -> Result<Box<dyn Probe + 'a>> {
        let cache = SharedScope::from(RedisBlockStore::with_prefix(
            self.store.clone(),
            &format!("{}", key),
        ));
        let lazy_probe = LazyProbe {
            inner: self.to_inner()?,
            key: (*key).clone(),
            cache,
        };
        Ok(Box::new(lazy_probe))
    }
    async fn get_records(&self, ctx_key: &ContextKey) -> Result<ContentStream<ArrowRecordBatch>> {
        self.to_inner()?.get_records(ctx_key).await
    }
    async fn get_schema(&self, ctx_key: &ContextKey) -> Result<ArrowSchema> {
        self.to_inner()?.get_schema(ctx_key).await
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use crate::opt::{Context, RelT, TableMeta, Validator};

    use tokio::runtime::Runtime;

    use crate::backends::Backend;

    const SIMPLE_QUERIES: &[&'static str] = &[
        "SELECT categories, SUM(review_count)
         FROM test_backend.business
         GROUP BY categories",
        "SELECT categories, AVG(useful)
         FROM test_backend.business a
         JOIN test_backend.review b
         ON a.business_id = b.business_id
         GROUP BY categories",
    ];

    fn mk_step(rel_t: RelT, ctx: Context<TableMeta>) -> Step {
        let random_name = uuid::Uuid::new_v4().to_simple().to_string();
        let promise = ContextKey::with_name(&random_name).and_prefix("parallax_internal");
        Step {
            rel_t,
            ctx,
            promise,
        }
    }

    fn test_backend<B: Backend>(backend: B, ctx: Context<TableMeta>) {
        let ctx = ctx
            .into_iter()
            .map(|(key, table)| {
                (
                    ContextKey::with_name(key.name()).and_prefix("test_backend"),
                    table,
                )
            })
            .collect();
        let mut runtime = Runtime::new().unwrap();
        runtime.block_on(async move {
            let validator = Validator::new(&ctx);
            for query in SIMPLE_QUERIES.iter() {
                let rel_t = validator.validate_str(query).unwrap();
                let step = mk_step(rel_t, ctx.clone());
                backend.compute(step).await.unwrap();
            }
        })
    }

    #[cfg(feature = "google-bigquery")]
    #[test]
    fn big_query() {
        test_backend(
            bigquery::tests::mk_big_query(),
            bigquery::tests::mk_context(),
        )
    }
}
