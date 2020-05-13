use crate::common::*;

use crate::backends::{LastUpdated, Probe};
use crate::gcp::bigquery::{JobBuilder, TableRef};
use crate::opt::{
    Context, ContextError, ContextKey, DataType, Domain, ExprMeta, MaximumFrequency, Mode,
    RowCount, TableMeta,
};
use crate::Result;

use super::{table_ref_to_context_key, BigQuery};

use yup_oauth2::GetToken;

use google_bigquery2::TableFieldSchema;

pub fn from_bigquery_type(ty: &str) -> Result<DataType> {
    match ty {
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
    }
}

pub(super) struct BigQueryProbe<'a, O> {
    inner: &'a BigQuery<O>,
    table_ref: TableRef,
    schema: HashMap<String, TableFieldSchema>,
}

impl<'a, O> BigQueryProbe<'a, O>
where
    O: GetToken + Send + Sync + 'static,
{
    pub(super) async fn new<'b>(
        inner: &'b BigQuery<O>,
        table_ref: TableRef,
    ) -> Result<BigQueryProbe<'b, O>> {
        let schema = inner
            .to_inner()
            .get_table(&table_ref)
            .await?
            .schema
            .and_then(|schema| schema.fields)
            .ok_or(Error::new("invalid response from BigQuery"))?
            .into_iter()
            .map(|field| {
                let name = field
                    .name
                    .as_ref()
                    .ok_or(Error::new("field does not have a name"))?
                    .clone();
                Ok((name, field))
            })
            .collect::<Result<_>>()?;
        Ok(BigQueryProbe {
            inner,
            table_ref,
            schema,
        })
    }
}

#[async_trait]
impl<'a, O> Probe for BigQueryProbe<'a, O>
where
    O: GetToken + Send + Sync + 'static,
{
    async fn expr(&self, key: &ContextKey) -> Result<ExprMeta> {
        let name = key.name();
        let field = self.schema.get(name).ok_or(
            ContextError::NotFound(key.clone())
                .into_column_error()
                .into_error(),
        )?;

        let ty = field
            .type_
            .as_ref()
            .ok_or(Error::new("expected a field type"))
            .and_then(|type_| from_bigquery_type(type_.as_str()))?;

        let mode = field
            .mode
            .as_ref()
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

        Ok(expr_meta)
    }
    async fn maximum_frequency(&self, key: &ContextKey) -> Result<MaximumFrequency> {
        let query_str = format!(
            "SELECT MAX(freq) \
             FROM (\
               SELECT {key} as key, \
                      COUNT(*) as freq \
               FROM {table_ref} \
               GROUP BY {key} \
             )",
            key = key.name(),
            table_ref = self.table_ref
        );

        let results = self.inner.lite_query(&query_str).await?;

        results
            .rows
            .and_then(|mut rows| {
                let mut cells = rows.pop()?.f?;
                cells.pop()?.v?.parse::<u64>().ok()
            })
            .ok_or(Error::new("invalid response from BigQuery"))
            .map(|v| MaximumFrequency::from(v))
    }
    async fn row_count(&self) -> Result<RowCount> {
        let query_str = format!("SELECT COUNT(*) FROM {}", self.table_ref);

        let results = self.inner.lite_query(&query_str).await?;

        results
            .rows
            .and_then(|mut rows| {
                let mut cells = rows.pop()?.f?;
                cells.pop()?.v?.parse::<u64>().ok()
            })
            .ok_or(Error::new("Invalid response from BigQuery"))
            .map(|v| RowCount::from(v))
    }
    async fn domain(&self, key: &ContextKey) -> Result<Domain> {
        let name = key.name();
        let meta = self.expr(key).await?;
        if meta.ty.is_numeric() {
            let query_str = format!(
                "SELECT MAX({}), MIN({}) FROM {}",
                name, name, self.table_ref
            );

            let results = self.inner.lite_query(&query_str).await?;

            let domain: Option<_> = try {
                let (min, max) = results.rows.and_then(|mut rows| {
                    let mut cells = rows.pop()?.f?;
                    let min = cells.pop()?.v?;
                    let max = cells.pop()?.v?;
                    Some((min, max))
                })?;
                match meta.ty {
                    DataType::Integer => {
                        let min = min.parse().ok()?;
                        let max = max.parse().ok()?;
                        Domain::Discrete { min, max, step: 1 }
                    }
                    DataType::Float => {
                        let min = min.parse().ok()?;
                        let max = max.parse().ok()?;
                        Domain::Continuous { min, max }
                    }
                    _ => unreachable!(),
                }
            };
            domain.ok_or(Error::new("invalid response from BigQuery"))
        } else {
            Ok(Domain::Opaque)
        }
    }
    async fn to_meta(&self) -> Result<TableMeta> {
        let mut columns = Context::new();
        for field in self.schema.keys() {
            let key = ContextKey::with_name(field);
            let expr_meta = self.expr(&key).await?;
            columns.insert(key, expr_meta);
        }
        let source = Some(table_ref_to_context_key(&self.table_ref));
        Ok(TableMeta {
            columns,
            source,
            ..Default::default()
        })
    }
    async fn last_updated(&self) -> Result<LastUpdated> {
        let t = std::time::SystemTime::now();

        let (project_id, dataset_id, table_id) = self.table_ref.unwrap();
        let query_str = format!(
            "SELECT last_modified_time \
             FROM {project_id}.{dataset_id}.__TABLES__ where table_id = '{table_id}'",
            project_id = project_id,
            dataset_id = dataset_id,
            table_id = table_id
        );

        let results = self.inner.lite_query(&query_str).await?;

        debug!(
            "Time taken for last_updated: {}ms",
            t.elapsed().unwrap().as_millis()
        );

        results
            .rows
            .and_then(|mut rows| {
                let mut cells = rows.pop()?.f?;
                let last_modified_time = cells.pop()?.v?;
                last_modified_time.parse::<u64>().ok()
            })
            .ok_or(Error::new("invalid response from backend"))
            .map(|last_modified_time| last_modified_time.into())
    }
}
