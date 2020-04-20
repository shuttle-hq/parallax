use crate::common::{Job as ApiJob, *};
use crate::node::{Access, AccessResult, Backends, Node};
use crate::Result;

use super::{Asset, Foreman, Job, JobStage, QueryDoneStage, QueryInitialisedStage, QueryStages};

pub struct Processor<A> {
    access: A,
    task: String,
    id: String,
}

impl<A> Processor<A>
where
    A: Access + 'static,
{
    pub fn new(access: A, task: String) -> Self {
        let id = Uuid::new_v4().to_simple().to_string();
        Self { access, task, id }
    }

    pub async fn get_output_schema(&self) -> AccessResult<ArrowSchema> {
        debug!("fetching output schema for {}", self.id);
        if let Some(output) = self.output()? {
            let schema = self
                .access
                .backend(&output.loc)
                .map_err(|e| self.access.error(e))?
                .get_schema(&output.source)
                .await
                .map_err(|e| self.access.error(e))?;
            Ok(schema)
        } else {
            Err(AccessError {
                kind: AccessErrorKind::NotFound as i32,
                ..Default::default()
            }
            .into())
        }
    }

    pub async fn get_output_rows(&self) -> AccessResult<ContentStream<ArrowRecordBatch>> {
        debug!("fetching output rows for {}", self.id);
        if let Some(output) = self.output()? {
            let rows = self
                .access
                .backend(&output.loc)
                .map_err(|e| self.access.error(e))?
                .get_records(&output.source)
                .await
                .map_err(|e| self.access.error(e))?;
            Ok(rows)
        } else {
            Err(AccessError {
                kind: AccessErrorKind::NotFound as i32,
                ..Default::default()
            })
        }
    }

    pub fn to_proto(&self) -> AccessResult<ApiJob> {
        self.state().map(|state| state.into_proto())
    }

    pub fn output(&self) -> AccessResult<Option<Asset>> {
        self.state().map(|job| {
            job.state.ok().and_then(|stage| match stage {
                JobStage::Query(QueryStages::Done(QueryDoneStage { asset })) => Some(asset),
                _ => None,
            })
        })
    }

    pub fn state(&self) -> AccessResult<Job> {
        self.access
            .job(&self.task)
            .map_err(|e| self.access.error(e))
            .and_then(|maybe_job| {
                maybe_job.ok_or(AccessError {
                    kind: AccessErrorKind::NotFound as i32,
                    ..Default::default()
                })
            })
    }

    pub fn acquire(&self) -> Result<Job> {
        trace!("acquiring task={} for id={}", self.task, self.id);
        self.mutate_state(|mut job| {
            let foreman = self.to_foreman()?;
            job.foreman = Some(foreman);
            let job = job.clone();
            Ok(job)
        })
    }

    pub fn release(&self) -> Result<Job> {
        trace!("releasing task={} from id={}", self.task, self.id);
        self.mutate_state(|mut job| {
            if job.foreman.is_some() {
                job.foreman = None;
            }
            let job = job.clone();
            Ok(job)
        })
    }

    pub async fn process_to_end(self) {
        let mut done = false;
        while !done {
            let job = self.poll().await.unwrap();
            done = job.is_done();
        }

        info!("Job done!")
    }

    pub async fn poll(&self) -> Result<Job> {
        trace!("polled foreman id={}", self.id);
        let mut job = self.acquire()?;
        info!("Polling job {:?}", job);
        if let Ok(state) = job.state {
            let state = state.advance(&self.access).await;
            self.mutate_state(|mut job| {
                job.state = state;
                Ok(())
            })?;
        }
        self.release()
    }

    pub async fn start(self, from: ApiJob) -> std::result::Result<ApiJob, AccessError> {
        let state = self
            .access
            .shared_job(&self.task)
            .map_err(|e| self.access.error(e))?;
        let mut lock = state.write().map_err(|e| self.access.error(e))?;

        if lock.is_some() {
            state.abort(lock).unwrap();
            let err = ScopeError::already_exists(&block_type!("job".(&self.task)));
            Err(self.access.error(err))
        } else {
            let query = from.query;

            let init = Job {
                id: self.task.clone(),
                timestamp: Utc::now(),
                state: Ok(JobStage::Query(QueryStages::Initialised(
                    QueryInitialisedStage { query },
                ))),
                user: self.access.who_am_i().to_string(),
                foreman: None,
            };

            let job = init.to_proto();

            *lock = Some(init);

            state.push(lock).unwrap();

            tokio::spawn(self.process_to_end());

            Ok(job)
        }
    }

    pub async fn cancel(&self) -> AccessResult<()> {
        unimplemented!()
    }

    fn to_foreman(&self) -> AccessResult<Foreman> {
        let peer = self
            .access
            .peer()
            .map_err(|e| self.access.error(e))?
            .block_type()
            .unwrap(); // safe (see impl Block for Peer)
        let id = self.id.clone();
        Ok(Foreman { peer, id })
    }

    fn mutate_state<O, F: FnOnce(&mut Job) -> Result<O>>(&self, f: F) -> Result<O> {
        let ty = block_type!("job".(&self.task));
        let state = self.access.shared_job(&self.task)?;
        let mut lock = state.write()?;

        let res = if let Some(job) = lock.as_mut() {
            if let Some(existing) = job.foreman.as_ref() {
                if *existing != self.to_foreman()? {
                    let description =
                        format!("job is already owned by another foreman: {:?}", existing);

                    let err = ScopeError {
                        kind: ScopeErrorKind::AlreadyExists as i32,
                        source: ty.to_string(),
                        description,
                    };

                    Err(err.into())
                } else {
                    f(job)
                }
            } else {
                f(job)
            }
        } else {
            Err(ScopeError::not_found(&ty).into())
        };

        match res {
            Ok(val) => {
                trace!("pushing new job state");
                state.push(lock).unwrap();
                Ok(val)
            }
            Err(err) => {
                trace!("aborting mutation of job state");
                state.abort(lock).unwrap();
                Err(err)
            }
        }
    }
}
