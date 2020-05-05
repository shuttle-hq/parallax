use anyhow::{Error, Result};

use std::io::{Cursor, Write};

use prettytable::Table;

use tonic::Request;

use futures::prelude::TryStream;
use futures::stream::{StreamExt, TryStreamExt};

use arrow::csv::writer::Writer as CsvWriter;
use arrow::ipc::reader::StreamReader;

use parallax_api::{
    client::Client, ArrowRecordBatch, ArrowSchema, GetJobOutputRowsRequest,
    GetJobOutputRowsResponse, GetJobOutputSchemaRequest, GetJobOutputSchemaResponse,
    InsertJobRequest, InsertJobResponse, Job, JobState, ListJobsRequest,
};

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

pub(crate) fn print_jobs(jobs: Vec<Job>) {
    let mut table = Table::new();
    table.add_row(row!["ID", "TIMESTAMP", "STATE"]);

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

pub struct Fetch<'a> {
    client: &'a mut Client,
    job: &'a Job,
    truncate: Option<usize>,
}

impl<'a> Fetch<'a> {
    pub fn new(client: &'a mut Client, job: &'a Job) -> Self {
        Self {
            client,
            job,
            truncate: None,
        }
    }
    pub fn truncate(mut self, to: usize) -> Self {
        self.truncate = Some(to);
        self
    }
    pub async fn write_csv<W: Write>(self, write: W) -> Result<()> {
        let serialized_schema = fetch_serialized_schema(self.client, &self.job).await?;

        let serialized_schema = strip_continuation_bytes(&serialized_schema)?;

        let mut serialized_rows = fetch_serialized_rows(self.client, &self.job).await?.boxed();

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
