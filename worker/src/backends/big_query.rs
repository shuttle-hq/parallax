use yup_oauth2::GetToken;

use google_bigquery2::{TableListTables, TableReference};

use bigquery_storage::client::{Client as BigQueryStorageClient, Error as ClientError};
use bigquery_storage::proto::bigquery_storage::read_rows_response::Rows;
use bigquery_storage::proto::bigquery_storage::read_session::Schema as ReadSchema;
use bigquery_storage::proto::bigquery_storage::{ReadRowsRequest, ReadRowsResponse};

use derive_more::From;

use super::Backend;
use crate::common::*;
use crate::gcp::errors::GcpError;
use crate::gcp::{
    bigquery::{BigQuery as BigQueryClient, JobBuilder, TableRef},
    oauth::Connector,
    Client,
};
use crate::Result;

use crate::opt::{
    plan::Step, rel::*, Context, ContextError, ContextKey, DataType, ExprMeta, Mode, RelAnsatz,
    ToAnsatz, ValidateError, ValidateResult,
};

pub struct BigQuery<O = ()> {
    dataset: DatasetId,
    cache: DatasetId,
    big_query: BigQueryClient<O>,
    storage: BigQueryStorage<O>,
}

#[derive(Clone)]
pub struct DatasetId {
    project_id: String,
    dataset_id: String,
}

impl DatasetId {
    fn into_ident(self) -> Vec<String> {
        vec![self.project_id, self.dataset_id]
    }
}

impl BigQuery<()> {
    pub fn new(resource: BigQueryBackend) -> Result<BigQuery<impl GetToken>> {
        let connector = Connector::builder()
            .with_key_file(Path::new(&resource.service_account_key))
            .set_allow_from_metadata(false)
            .build()
            .map_err(|e| BackendError::from(e))?;

        let gcp_client = Client::new(connector);

        let big_query =
            BigQueryClient::new_with_credentials(&resource.staging_project_id, gcp_client);

        let storage_connector = Connector::builder()
            .with_key_file(Path::new(&resource.service_account_key))
            .set_allow_from_metadata(false)
            .build()
            .map_err(|e| BackendError::from(e))?;

        let storage = BigQueryStorage::new(storage_connector);

        let dataset = DatasetId {
            project_id: resource.project_id.clone(),
            dataset_id: resource.dataset_id.clone(),
        };

        let cache = DatasetId {
            project_id: if resource.staging_project_id.is_empty() {
                resource.project_id
            } else {
                resource.staging_project_id
            },
            dataset_id: if resource.staging_dataset_id.is_empty() {
                resource.dataset_id
            } else {
                resource.staging_dataset_id
            },
        };

        let exec = BigQuery {
            dataset,
            cache,
            big_query,
            storage,
        };

        Ok(exec)
    }
}

impl<O> BigQuery<O>
where
    O: GetToken + Send + Sync,
{
    fn to_inner(&self) -> &BigQueryClient<O> {
        &self.big_query
    }

    pub async fn get_context_from_backend(
        &self,
        project_id: &str,
        dataset_id: &str,
    ) -> Result<Context<TableMeta>> {
        let tables = self
            .to_inner()
            .list_tables(project_id, dataset_id)
            .await?
            .tables
            .ok_or(Error::new("expected a tables list"))?;

        let mut ctx = Context::new();

        for table in tables {
            let table_id = match table {
                TableListTables {
                    table_reference:
                        Some(TableReference {
                            table_id: Some(table_id),
                            ..
                        }),
                    ..
                } => table_id,
                _ => return Err(Error::new("expected a table_id")),
            };

            let context_key = ContextKey::with_name(&table_id);
            let table_meta = self
                .get_table_from_backend(project_id, dataset_id, &table_id)
                .await?;
            ctx.insert(context_key, table_meta);
        }

        Ok(ctx)
    }

    async fn get_table_from_backend(
        &self,
        project_id: &str,
        dataset_id: &str,
        table_id: &str,
    ) -> Result<TableMeta> {
        let table = self
            .to_inner()
            .get_table(project_id, dataset_id, table_id)
            .await?;

        let schema = table
            .schema
            .and_then(|s| s.fields)
            .ok_or(Error::new("expected a schema"))?;

        let columns: Context<ExprMeta> = schema
            .into_iter()
            .map(|field| {
                let name = field.name.ok_or(Error::new("expected a field name"))?;
                let context_key = ContextKey::with_name(&name);

                let ty = field
                    .type_
                    .ok_or(Error::new("expected a field type"))
                    .and_then(|type_| match type_.as_str() {
                        "STRING" => Ok(DataType::String),
                        "BYTES" => Ok(DataType::Bytes),
                        "INTEGER" | "INT64" => Ok(DataType::Integer),
                        "FLOAT" | "FLOAT64" => Ok(DataType::Float),
                        "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
                        "DATETIME" | "TIMESTAMP" => Ok(DataType::Timestamp),
                        "DATE" => Ok(DataType::Date),
                        type_str => {
                            let err_msg = format!("BigQuery type '{}'", type_str);
                            let err = Error::new_detailed(
                                "BigQuery data type not supported",
                                NotSupportedError { feature: err_msg },
                            );
                            Err(err)
                        }
                    })?;

                let mode = field
                    .mode
                    .map(|mode| match mode.as_str() {
                        "NULLABLE" => Ok(Mode::Nullable),
                        "REQUIRED" => Ok(Mode::Required),
                        mode_str => {
                            let err_msg = format!("BigQuery mode '{}'", mode_str);
                            let err = Error::new_detailed(
                                "BigQuery table mode",
                                NotSupportedError { feature: err_msg },
                            );
                            Err(err)
                        }
                    })
                    .transpose()?
                    .unwrap_or(Mode::Nullable);

                let expr_meta = ExprMeta {
                    ty,
                    mode,
                    ..Default::default()
                };
                Ok((context_key, expr_meta))
            })
            .collect::<Result<_>>()?;

        let mut table_meta = TableMeta::from(columns).unwrap();
        let context_key = ContextKey::with_name(table_id)
            .and_prefix(dataset_id)
            .and_prefix(project_id);
        table_meta.source = Some(context_key);
        Ok(table_meta)
    }
}

#[tonic::async_trait]
impl<O> Backend for BigQuery<O>
where
    O: GetToken + Send + Sync + 'static,
{
    async fn compute(&self, step: Step) -> Result<()> {
        // take RelT into a SQL string and execute against BQ
        let ctx = step
            .ctx
            .into_iter()
            .map(|(ck, meta)| {
                let DatasetId {
                    project_id,
                    dataset_id,
                } = self.dataset.clone();
                let table_id = meta
                    .source
                    .ok_or(Error::new("a table had no associated context_key"))?
                    .name()
                    .to_string();
                let table = BigQueryTable {
                    project_id,
                    dataset_id,
                    table_id,
                };
                Ok((ck, table))
            })
            .collect::<Result<_>>()?;
        let rel_t = BigQueryRelT::wrap(step.rel_t, &ctx);
        let query: sqlparser::ast::Query = rel_t.to_ansatz()?.into();
        let query_str = query.to_string();

        let output = TableRef {
            project_id: self.cache.project_id.clone(),
            dataset_id: self.cache.dataset_id.clone(),
            table_id: step.promise.name().to_string(),
        };

        let mut builder = JobBuilder::default();
        builder
            .project_id(&self.cache.project_id.clone())
            .query(&query_str, output);
        let job_request = builder.build()?;
        let job = self.big_query.run_to_completion(job_request).await?;
        Ok(())
    }

    async fn get_meta(&self, ty: &ContextKey) -> Result<TableMeta> {
        let DatasetId {
            project_id,
            dataset_id,
        } = &self.dataset;
        let meta = self
            .get_table_from_backend(project_id, dataset_id, ty.name())
            .await?;
        Ok(meta)
    }

    async fn get_schema(&self, ctx_key: &ContextKey) -> Result<ArrowSchema> {
        let table_ref = TableRef {
            project_id: self.cache.project_id.clone(),
            dataset_id: self.cache.dataset_id.clone(),
            table_id: ctx_key.name().to_string(),
        };
        self.storage
            .get_schema(&table_ref)
            .await
            .map_err(|e| e.into())
    }

    async fn get_records(&self, ctx_key: &ContextKey) -> Result<ContentStream<ArrowRecordBatch>> {
        let table_ref = TableRef {
            project_id: self.cache.project_id.clone(),
            dataset_id: self.cache.dataset_id.clone(),
            table_id: ctx_key.name().to_string(),
        };
        self.storage
            .stream_rows(&table_ref)
            .await
            .map_err(|e| e.into())
    }
}

impl From<GcpError> for Error {
    fn from(gcpe: GcpError) -> Self {
        Error {
            reason: format!("GcpError: {}", gcpe.to_string()),
            ..Default::default()
        }
    }
}

impl From<ContextError> for Error {
    fn from(ce: ContextError) -> Self {
        Error {
            reason: format!("ContextError: {}", ce.to_string()),
            ..Default::default()
        }
    }
}

pub struct BigQueryTable {
    project_id: String,
    dataset_id: String,
    table_id: String,
}

impl BigQueryTable {
    fn to_context_key(&self) -> ContextKey {
        ContextKey::with_name(&self.table_id)
            .and_prefix(&self.dataset_id)
            .and_prefix(&self.project_id)
    }
}

pub struct BigQueryRelT<'a> {
    root: RelT,
    ctx: &'a Context<BigQueryTable>,
}

impl<'a> BigQueryRelT<'a> {
    fn wrap(root: RelT, ctx: &'a Context<BigQueryTable>) -> Self {
        Self { ctx, root }
    }

    fn to_ansatz(self) -> std::result::Result<RelAnsatz, ContextError> {
        let ctx = self.ctx;
        self.root.try_fold(&mut |t| {
            match t {
                Rel::Table(Table(key)) => {
                    ctx.look_up(&key)
                        .map(|loc| Rel::Table(Table(loc.to_context_key())))
                        .map(|t| t.to_ansatz().unwrap()) // FIXMEs
                }
                _ => Ok(t.to_ansatz().unwrap()),
            }
        })
    }
}

#[derive(From, Debug)]
pub enum BigQueryStorageError {
    BigQueryStorage(ClientError),
    MissingSchema,
    #[from(ignore)]
    MalformedData(String),
}

impl Into<Error> for BigQueryStorageError {
    fn into(self) -> Error {
        Error {
            reason: format!("{:?}", self),
            ..Default::default()
        }
    }
}

pub struct BigQueryStorage<T>(BigQueryStorageClient<T>);

impl<T> BigQueryStorage<T>
where
    T: GetToken + Send + Sync + 'static,
{
    pub fn new(connector: T) -> Self {
        let client = BigQueryStorageClient::new(connector);
        Self(client)
    }

    pub async fn get_schema(
        &self,
        table_ref: &TableRef,
    ) -> std::result::Result<ArrowSchema, BigQueryStorageError> {
        let TableRef {
            project_id,
            dataset_id,
            table_id,
        } = table_ref;
        let read_session = self
            .0
            .create_read_session(project_id, dataset_id, table_id)
            .await?;

        let schema = read_session
            .schema
            .ok_or(BigQueryStorageError::MissingSchema)?;

        match schema {
            ReadSchema::ArrowSchema(arrow_schema) => Ok(ArrowSchema {
                serialized_schema: arrow_schema.serialized_schema,
            }),
            _ => Err(BigQueryStorageError::MalformedData(
                "Was expecting Arrow Schema, got Avro.".to_string(),
            )),
        }
    }

    pub async fn stream_rows(
        &self,
        table_ref: &TableRef,
    ) -> std::result::Result<ContentStream<ArrowRecordBatch>, BigQueryStorageError> {
        let TableRef {
            project_id,
            dataset_id,
            table_id,
        } = table_ref;
        let read_session = self
            .0
            .create_read_session(project_id, dataset_id, table_id)
            .await?;

        let mut streams = Vec::<tonic::Streaming<ReadRowsResponse>>::new();
        for stream in read_session.streams.iter() {
            let read_row_request = ReadRowsRequest {
                read_stream: stream.name.clone(),
                offset: 0,
            };
            let mut row_response_stream = self.0.read_rows(read_row_request.clone()).await.unwrap();
            streams.push(row_response_stream);
        }

        // Retrieves rows from streams
        // TODO parallelize
        let stream = async_stream::try_stream! {
            for mut row_response_stream in streams {
                while let Ok(Some(row_response)) = row_response_stream.message().await {
                    match row_response.rows.unwrap() {
                        Rows::ArrowRecordBatch(arrow_record_batch) => {

                            let serialized_record_batch = arrow_record_batch.serialized_record_batch;
                            let row_count = arrow_record_batch.row_count;

                            yield ArrowRecordBatch {
                                serialized_record_batch,
                                row_count
                            }

                        }
                        _ => { unimplemented!("Todo") }
                    }
                }
            }
        };

        Ok(Box::pin(stream) as ContentStream<ArrowRecordBatch>)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::gcp::Client;

    const PROJECT_ID: &'static str = "openquery-dev";
    const TEST_DATASET_ID: &'static str = "yelp";
    const STAGING_DATASET_ID: &'static str = "cache";

    const BIGQUERY_SCOPE_PREFIX: &'static str = "parallax_bigquery_testing";

    pub fn mk_context() -> Context<TableMeta> {
        let big_query = mk_big_query();
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            big_query
                .get_context_from_backend(PROJECT_ID, TEST_DATASET_ID)
                .await
                .unwrap()
        })
    }

    pub fn mk_big_query() -> BigQuery<impl GetToken> {
        let gcp_connector = crate::gcp::tests::mk_connector();
        let gcp_client = Client::new(gcp_connector);
        let client = BigQueryClient::new_with_credentials(PROJECT_ID, gcp_client);

        let storage_connector = crate::gcp::tests::mk_connector();
        let storage_client = BigQueryStorage::new(storage_connector);
        BigQuery {
            dataset: DatasetId {
                project_id: PROJECT_ID.to_string(),
                dataset_id: TEST_DATASET_ID.to_string(),
            },
            cache: DatasetId {
                project_id: PROJECT_ID.to_string(),
                dataset_id: STAGING_DATASET_ID.to_string(),
            },
            big_query: client,
            storage: storage_client,
        }
    }
}
