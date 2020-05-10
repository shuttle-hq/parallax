use google_bigquery2::{
    Bigquery, GetQueryResultsResponse, Job as BigQueryJob, JobConfiguration,
    JobConfigurationExtract, JobConfigurationLoad, JobConfigurationQuery, JobStatus, QueryRequest,
    Table, TableList, TableReference,
};

use crate::gcp::{self, errors::*, oauth};

use yup_oauth2::GetToken;

use http::{method::Method, uri::Uri};

use crate::opt::ContextKey;
use futures::stream::StreamExt;
use std::convert::TryFrom;
use std::string::ToString;
use std::time::Duration;
use tokio::time;

#[derive(Default)]
pub struct JobBuilder {
    endpoint: Option<String>,
    project_id: Option<String>,
    method: Option<Method>,
    configuration: Option<JobConfiguration>,
}

pub struct JobRequest {
    uri: String,
    method: Method,
    project_id: String,
    job: Option<BigQueryJob>,
}

#[derive(Debug, Clone)]
pub struct TableRef {
    project_id: String,
    dataset_id: String,
    table_id: String,
}

impl TableRef {
    pub fn new(project_id: &str, dataset_id: &str, table_id: &str) -> Result<Self> {
        if Self::is_valid(dataset_id)
            && Self::is_valid(table_id)
            && Self::is_valid_project(project_id)
        {
            Ok(Self {
                project_id: project_id.to_string(),
                dataset_id: dataset_id.to_string(),
                table_id: table_id.to_string(),
            })
        } else {
            let err_msg = format!(
                "{}:{}.{} is not a valid BigQuery table name",
                project_id, dataset_id, table_id
            );
            Err(GcpError::ApiError(err_msg))
        }
    }
    pub fn unwrap(&self) -> (&str, &str, &str) {
        (&self.project_id, &self.dataset_id, &self.table_id)
    }
    fn is_valid(s: &str) -> bool {
        s.chars().all(|c| c.is_alphanumeric() || c == '_')
    }
    fn is_valid_project(s: &str) -> bool {
        s.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    }
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "`{}`.`{}`.`{}`",
            self.project_id, self.dataset_id, self.table_id
        )
    }
}

impl Into<TableReference> for TableRef {
    fn into(self) -> TableReference {
        TableReference {
            project_id: Some(self.project_id),
            dataset_id: Some(self.dataset_id),
            table_id: Some(self.table_id),
        }
    }
}

impl JobBuilder {
    pub fn project_id(&mut self, project_id: &str) -> &mut Self {
        self.project_id = Some(project_id.to_string());
        self
    }
    pub fn get(&mut self, job_id: &str) -> &mut Self {
        self.method = Some(Method::GET);
        self.endpoint = Some(format!("/{}", job_id));
        self
    }
    pub fn extract(&mut self, input_table: TableRef, output_uri: &str) -> &mut Self {
        self.endpoint = None;
        self.method = Some(Method::POST);
        let extract_job_conf = JobConfigurationExtract {
            source_table: Some(input_table.into()),
            destination_uris: Some(vec![output_uri.to_string()]),
            destination_format: Some("NEWLINE_DELIMITED_JSON".to_string()),
            compression: None,
            print_header: Some(true),
            field_delimiter: Some(",".to_string()),
            destination_uri: None, // Deprecated over destination_uris,
            ..Default::default()
        };

        let job_conf = JobConfiguration {
            extract: Some(extract_job_conf),
            ..Default::default()
        };
        self.configuration = Some(job_conf);
        self
    }
    pub fn load(&mut self, input_uri: &str, output_table: TableRef) -> &mut Self {
        self.endpoint = None;
        self.method = Some(Method::POST);
        let load_job_conf = JobConfigurationLoad {
            source_uris: Some(vec![input_uri.to_string()]),
            destination_table: Some(output_table.into()),
            ..Default::default()
        };
        let job_conf = JobConfiguration {
            load: Some(load_job_conf),
            ..Default::default()
        };
        self.configuration = Some(job_conf);
        self
    }
    pub fn query(&mut self, sql: &str, output: TableRef) -> &mut Self {
        self.endpoint = None;
        self.method = Some(Method::POST);
        let query_job_conf = JobConfigurationQuery {
            query: Some(sql.to_string()),
            destination_table: Some(output.into()),
            use_legacy_sql: Some(false),
            ..Default::default()
        };
        let job_conf = JobConfiguration {
            query: Some(query_job_conf),
            ..Default::default()
        };
        self.configuration = Some(job_conf);
        self
    }

    pub fn build(self) -> Result<JobRequest> {
        let method = self
            .method
            .ok_or(GcpError::JobBuilderError("missing a method".to_string()))?;
        let endpoint = self.endpoint.unwrap_or("".into());
        let project_id = self.project_id.ok_or(GcpError::JobBuilderError(
            "missing a host project_id".to_string(),
        ))?;
        let uri = format!(
            "https://bigquery.googleapis.com/bigquery/v2\
             /projects/{}/jobs{}",
            project_id, endpoint
        );

        let job = if method == Method::GET {
            None
        } else {
            let config = self.configuration.ok_or(GcpError::JobBuilderError(
                "need one of load, query or extract".to_string(),
            ))?;
            Some(BigQueryJob {
                configuration: Some(config),
                ..Default::default()
            })
        };

        Ok(JobRequest {
            project_id,
            uri,
            method,
            job,
        })
    }
}
// Add functionality here to export credentials or whatver is necessary.
pub struct BigQuery<O = ()> {
    project_id: String,
    gcp: gcp::Client<O>,
}

impl<O> BigQuery<O> {
    pub fn new_with_credentials(project_id: &str, client: gcp::Client<O>) -> BigQuery<O> {
        BigQuery::<_> {
            project_id: project_id.to_string(),
            gcp: client,
        }
    }
}

impl<O> BigQuery<O> {
    pub fn job(&self) -> JobBuilder {
        JobBuilder {
            project_id: Some(self.project_id.clone()),
            ..JobBuilder::default()
        }
    }
}

impl<O> BigQuery<O>
where
    O: GetToken,
{
    pub async fn list_tables(&self, project_id: &str, dataset_id: &str) -> Result<TableList> {
        let uri = format!(
            "https://bigquery.googleapis.com/bigquery/v2\
             /projects/{}\
             /datasets/{}\
             /tables",
            project_id, dataset_id
        );
        self.gcp
            .api_request::<(), _>(Method::GET, uri.parse()?, None)
            .await
    }
    pub async fn get_table(&self, table_ref: &TableRef) -> Result<Table> {
        let (project_id, dataset_id, table_id) = table_ref.unwrap();
        let uri = format!(
            "https://bigquery.googleapis.com/bigquery/v2\
             /projects/{}\
             /datasets/{}\
             /tables/{}",
            project_id, dataset_id, table_id
        );
        self.gcp
            .api_request::<(), _>(Method::GET, uri.parse()?, None)
            .await
    }
    pub async fn send(&self, req: JobRequest) -> Result<BigQueryJob> {
        self.gcp
            .api_request(req.method.clone(), req.uri.parse()?, req.job)
            .await
    }
    pub async fn run_query(
        &self,
        staging_project_id: &str,
        staging_dataset_id: &str,
        query_str: &str,
    ) -> Result<BigQueryJob> {
        let rand_table_ref = TableRef::new(
            staging_project_id,
            staging_dataset_id,
            &uuid::Uuid::new_v4().to_simple().to_string(),
        )?;

        let mut builder = JobBuilder::default();
        builder
            .project_id(staging_project_id)
            .query(query_str, rand_table_ref);

        let job_request = builder.build()?;

        self.run_to_completion(job_request).await
    }
    pub async fn run_query_and_get_results(
        &self,
        staging_project_id: &str,
        staging_dataset_id: &str,
        query_str: &str,
    ) -> Result<GetQueryResultsResponse> {
        let job = self
            .run_query(staging_project_id, staging_dataset_id, query_str)
            .await?;

        let job_id = job
            .job_reference
            .and_then(|jr| jr.job_id)
            .ok_or(GcpError::ApiError(
                "invalid response from backend".to_string(),
            ))?;

        let results = self.get_query_results(staging_project_id, &job_id).await?;

        Ok(results)
    }
    pub async fn get_query_results(
        &self,
        project_id: &str,
        job_id: &str,
    ) -> Result<GetQueryResultsResponse> {
        let uri = format!(
            "https://bigquery.googleapis.com/bigquery/v2\
             /projects/{}/queries/{}",
            project_id, job_id
        );
        self.gcp
            .api_request::<(), _>(Method::GET, uri.parse()?, None)
            .await
    }
    pub async fn run_to_completion(&self, req: JobRequest) -> Result<BigQueryJob> {
        let mut job = self.send(req).await?;
        loop {
            time::delay_for(Duration::from_millis(1000)).await;
            let job_id =
                job.job_reference
                    .and_then(|jr| jr.job_id)
                    .ok_or(GcpError::InvalidResponse(
                        "API response did not include a 'job_reference.job_id'".to_string(),
                    ))?;
            let mut job_request_builder = self.job();
            job_request_builder.get(&job_id);
            let job_request = job_request_builder.build()?;
            job = self.send(job_request).await?;
            if job.clone().status.and_then(|s| s.state) == Some("DONE".to_string()) {
                return Ok(job);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::{oauth::Connector, tests::mk_connector, Client},
        BigQuery, TableRef,
    };
    use tokio::runtime::Runtime;

    use std::env;
    use std::path::Path;

    use uuid::Uuid;

    #[test]
    fn test_query() {
        let mut rt = Runtime::new().unwrap();

        let connector = mk_connector();

        let gcp_client = Client::new(connector);

        let mut bq = BigQuery::new_with_credentials("openquery-dev", gcp_client);

        let table_id = Uuid::new_v4().to_simple().to_string();

        rt.block_on(async move {
            let mut job_builder = bq.job();
            job_builder.query(
                "SELECT * FROM yelp.business",
                TableRef {
                    project_id: "openquery-dev".to_string(),
                    dataset_id: "cache".to_string(),
                    table_id: table_id,
                },
            );
            let job = job_builder
                .build()
                .map_err(|e| {
                    println!("failed to build job: {:?}", e);
                    e
                })
                .ok()
                .unwrap();
            bq.run_to_completion(job).await
        })
        .map_err(|e| {
            println!("failed to complete job: {:?}", e);
            e
        })
        .ok()
        .unwrap();
    }
}
