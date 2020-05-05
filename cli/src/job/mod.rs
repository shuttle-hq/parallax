use anyhow::{Error, Result};

use std::io::{Cursor, Write};

use prettytable::Table;

use tonic::Request;

use futures::prelude::{Stream, TryStream};
use futures::stream::{self, StreamExt, TryStreamExt};

use arrow::csv::writer::Writer as CsvWriter;
use arrow::ipc::reader::StreamReader;

use parallax_api::{
    client::Client, ArrowRecordBatch, ArrowSchema, GetJobOutputRowsRequest,
    GetJobOutputRowsResponse, GetJobOutputSchemaRequest, GetJobOutputSchemaResponse, GetJobRequest,
    GetJobResponse, InsertJobRequest, InsertJobResponse, Job, JobState, JobStatus, ListJobsRequest,
    QueryJobRequest, QueryJobResponse,
};
use std::pin::Pin;
use tonic::codegen::BoxStream;

pub fn is_done(job: &Job) -> bool {
    if let Some(status) = job.status.as_ref() {
        status.state() == JobState::Done
    } else {
        false
    }
}

pub async fn find_job(client: &mut Client, pattern: &str) -> Result<Job> {
    let jobs = list_jobs(client).await?;
    let matches: Vec<Job> = jobs
        .into_iter()
        .filter(|job| job.id.starts_with(pattern))
        .collect();
    match matches.len() {
        0 => Err(Error::msg(format!("job {} not found", pattern))),
        1 => Ok(matches.get(0).unwrap().to_owned()),
        _ => {
            let match_ids: Vec<String> = matches.into_iter().map(|job| job.id).collect();
            Err(Error::msg(format!(
                "more than one job with same id found {:?}",
                match_ids
            )))
        }
    }
}

pub async fn insert_job(client: &mut Client, query: &str) -> Result<Job> {
    let job = Job {
        query: query.to_string(),
        ..Default::default()
    };
    let req = Request::new(InsertJobRequest { job: Some(job) });
    let InsertJobResponse { job } = client.insert_job(req).await?.into_inner();
    let job = job.ok_or(Error::msg("no job was returned"))?;
    Ok(job)
}

pub async fn list_jobs(client: &mut Client) -> Result<Vec<Job>> {
    let req = Request::new(ListJobsRequest {});
    let jobs = client.list_jobs(req).await?.into_inner().jobs;
    Ok(jobs)
}

pub(crate) fn print_jobs(mut jobs: Vec<Job>) {
    let mut table = Table::new();
    table.add_row(row!["ID", "TIMESTAMP", "STATE"]);

    // Alphanumeric sorting should be ok here.
    jobs.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

    for job in jobs.into_iter() {
        let id = job.id;
        let timestamp = job.timestamp;
        let state = if let Some(status) = job.status {
            match status.state() {
                JobState::Done => {
                    if status.final_error.is_some() {
                        "Failed".to_string()
                    } else {
                        "Done".to_string()
                    }
                }
                other => format!("{:?}", other),
            }
        } else {
            "Unknown".to_string()
        };
        table.add_row(row![id, timestamp, state]);
    }

    table.printstd();
}

pub fn strip_continuation_bytes(msg: &[u8]) -> Result<&[u8]> {
    let header = msg
        .get(0..4)
        .ok_or(Error::msg("arrow message of invalid len"))?;
    if header != [255; 4] {
        Err(Error::msg("invalid arrow message"))
    } else {
        let tail = msg.get(4..).ok_or(Error::msg("empty arrow message"))?;
        Ok(tail)
    }
}

pub async fn fetch_serialized_schema(client: &mut Client, job: &Job) -> Result<Vec<u8>> {
    let req = Request::new(GetJobOutputSchemaRequest {
        job_id: job.id.clone(),
    });
    let GetJobOutputSchemaResponse { arrow_schema } =
        client.get_job_output_schema(req).await?.into_inner();

    let ArrowSchema { serialized_schema } =
        arrow_schema.ok_or(Error::msg("unexpected: did not receive output schema"))?;

    Ok(serialized_schema)
}

pub async fn fetch_serialized_rows(
    client: &mut Client,
    job: &Job,
) -> Result<impl TryStream<Ok = Vec<u8>, Error = Error, Item = Result<Vec<u8>>>> {
    let req = Request::new(GetJobOutputRowsRequest {
        job_id: job.id.clone(),
    });

    client
        .get_job_output_rows(req)
        .await
        .map_err(|e| e.into())
        .map(|resp| {
            resp.into_inner()
                .map_err(|status| {
                    Error::new(status).context("error receiving a batch of output records")
                })
                .and_then(
                    |GetJobOutputRowsResponse { arrow_record_batch }| async move {
                        let ArrowRecordBatch {
                            serialized_record_batch,
                            ..
                        } = arrow_record_batch
                            .ok_or(Error::msg("unexpected: did not receive records"))?;
                        Ok(serialized_record_batch)
                    },
                )
        })
}

pub enum FetchOp {
    Fetch,
    Query { timeout: u64 },
}

pub struct Fetch<'a> {
    client: &'a mut Client,
    job: &'a Job,
    truncate: Option<usize>,
    op: FetchOp,
}

impl<'a> Fetch<'a> {
    pub fn new(client: &'a mut Client, job: &'a Job, op: FetchOp) -> Self {
        Self {
            client,
            job,
            truncate: None,
            op,
        }
    }

    pub fn fetch(client: &'a mut Client, job: &'a Job) -> Self {
        Fetch::new(client, job, FetchOp::Fetch)
    }

    pub fn query(client: &'a mut Client, job: &'a Job, timeout: u64) -> Self {
        Fetch::new(client, job, FetchOp::Query { timeout })
    }

    pub fn truncate(mut self, to: usize) -> Self {
        self.truncate = Some(to);
        self
    }

    pub async fn write_csv<W: Write>(self, write: W) -> Result<()> {
        let (serialized_schema, mut serialized_rows) = match self.op {
            FetchOp::Fetch => {
                let serialized_schema = fetch_serialized_schema(self.client, &self.job).await?;
                let serialized_rows: Pin<Box<dyn Stream<Item = Result<Vec<u8>>>>> =
                    fetch_serialized_rows(self.client, &self.job).await?.boxed();
                (serialized_schema, serialized_rows)
            }
            FetchOp::Query { timeout } => {
                let query_job_response = self
                    .client
                    .query_job(QueryJobRequest {
                        job: Some(self.job.clone()),
                        timeout,
                    })
                    .await?
                    .into_inner();
                let serialized_schema: Vec<u8> = query_job_response
                    .arrow_schema
                    .ok_or(Error::msg("unexpected: did not receive output schema"))?
                    .serialized_schema
                    .to_owned();
                let serialized_rows: Vec<Result<Vec<u8>>> = query_job_response
                    .arrow_record_batches
                    .into_iter()
                    .map(|arb| Ok(arb.serialized_record_batch))
                    .collect();
                let row_stream_iter = Box::new(stream::iter(serialized_rows));
                let stream = row_stream_iter as Box<dyn Stream<Item = Result<Vec<u8>>>>;
                (serialized_schema, Pin::from(stream))
            }
        };

        let serialized_schema = strip_continuation_bytes(&serialized_schema)?;

        let mut writer = CsvWriter::new(write);

        let mut num_received = 0usize;

        while let Some(chunk) = serialized_rows.next().await {
            let chunk = chunk?;

            let mut buf = Vec::new();
            buf.write(&serialized_schema)?;

            let stripped_chunk = strip_continuation_bytes(chunk.as_slice())?;

            buf.write(stripped_chunk)?;

            let mut arrow_read = StreamReader::try_new(Cursor::new(&buf))?;
            let record_batch = arrow_read.next()?.unwrap();

            num_received += record_batch.num_rows();

            writer.write(&record_batch)?;

            if let Some(to) = self.truncate.as_ref() {
                if num_received >= *to {
                    break;
                }
            }
        }

        Ok(())
    }
}
