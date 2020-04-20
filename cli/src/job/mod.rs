use anyhow::{Error, Result};

use serde::{Deserialize, Serialize};

use std::io::{Cursor, Read, Write};
use std::sync::Arc;

use tonic::Request;

use futures::prelude::{Stream, TryStream};
use futures::stream::{StreamExt, TryStreamExt};

use arrow::csv::writer::Writer as CsvWriter;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;

use csv as csv_crate;

use parallax_api::{
    client::Client, ArrowRecordBatch, ArrowSchema, GetJobOutputRowsRequest,
    GetJobOutputRowsResponse, GetJobOutputSchemaRequest, GetJobOutputSchemaResponse, GetJobRequest,
    GetJobResponse, InsertJobRequest, InsertJobResponse, Job, JobState, JobStatus,
};

pub fn is_done(job: &Job) -> bool {
    if let Some(status) = job.status.as_ref() {
        status.state() == JobState::Done
    } else {
        false
    }
}

pub async fn get_job(client: &mut Client, job_id: &str) -> Result<Job> {
    let req = GetJobRequest {
        job_id: job_id.to_string(),
    };
    let GetJobResponse { job: new_job } = client.get_job(req).await?.into_inner();
    let new_job = new_job.ok_or(Error::msg("no job was returned"))?;
    Ok(new_job)
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Jobs(Vec<Job>);

impl std::iter::FromIterator<Job> for Jobs {
    fn from_iter<T: IntoIterator<Item = Job>>(iter: T) -> Self {
        Self(Vec::from_iter(iter))
    }
}

impl std::iter::IntoIterator for Jobs {
    type Item = Job;
    type IntoIter = std::vec::IntoIter<Job>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl Jobs {
    pub async fn insert(&mut self, client: &mut Client, query: &str) -> Result<Job> {
        let job = Job {
            query: query.to_string(),
            ..Default::default()
        };
        let req = Request::new(InsertJobRequest { job: Some(job) });
        let InsertJobResponse { job } = client.insert_job(req).await?.into_inner();
        let job = job.ok_or(Error::msg("no job was returned"))?;
        self.0.push(job.clone());
        Ok(job)
    }
    pub fn find(&mut self, job_id: &str) -> Result<&mut Job> {
        let mut matches: Vec<_> = self
            .0
            .iter_mut()
            .filter(|job| job.id.contains(job_id))
            .collect();
        match matches.len() {
            0 => Err(Error::msg(format!("job {} not found", job_id))),
            1 => Ok(matches.pop().unwrap()),
            _ => Err(Error::msg(format!("more than one job with same id found"))), // FIXME
        }
    }
    pub async fn refresh(&mut self, client: &mut Client) -> Result<()> {
        for job in self.0.iter_mut() {
            if is_done(job) {
                continue;
            }
            *job = get_job(client, &job.id).await?;
        }
        Ok(())
    }
}
