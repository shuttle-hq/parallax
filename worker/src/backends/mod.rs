use crate::common::*;
use crate::node::SharedState;
use crate::opt::{plan::Step, Context, ContextKey, TableMeta};
use crate::Result;

#[cfg(feature = "google-bigquery")]
pub mod big_query;

#[cfg(feature = "google-bigquery")]
pub use big_query::BigQuery;

#[tonic::async_trait]
pub trait Backend: Send + Sync {
    /// Compute the plan. Requirement: `stage.output()` is owned by self
    /// It is guaranteed that self.migrate(..) will have been called for each `data_id`
    /// in `stage.closure()`
    async fn compute(&self, stage: Step) -> Result<()>;

    async fn get_meta(&self, ty: &ContextKey) -> Result<TableMeta>;

    /// Retrieve the Results from the remote backend
    async fn get_records(&self, data_id: &ContextKey) -> Result<ContentStream<ArrowRecordBatch>>;

    async fn get_schema(&self, data_id: &ContextKey) -> Result<ArrowSchema>;
}

pub struct LazyBackend<C, E: ?Sized> {
    from_cache: C,
    ty: BlockType,
    exec: PhantomData<E>,
}

impl<C, E> LazyBackend<C, E>
where
    C: jac::Read<Item = Result<Arc<E>>, Error = jac::redis::Error> + Send + Sync,
    E: Backend + ?Sized,
{
    pub fn new(from_cache: C, ty: BlockType) -> Self {
        Self {
            from_cache,
            ty,
            exec: PhantomData,
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
    async fn get_meta(&self, ty: &ContextKey) -> Result<TableMeta> {
        self.to_inner()?.get_meta(ty).await
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
            .map(|(key, t)| (key.and_prefix("test_backend"), t))
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
            big_query::tests::mk_big_query(),
            big_query::tests::mk_context(),
        )
    }
}
