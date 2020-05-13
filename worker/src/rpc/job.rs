use crate::common::*;

const MAX_TIMEOUT_MS: u64 = 3600; // FIXME: expose as a state setting

use crate::node::{Access, AccessProvider};
use std::time::{Duration, Instant};
use tonic::metadata::KeyAndValueRef;

pub struct JobServiceImpl<A> {
    access: A,
}

impl<A> JobServiceImpl<A> {
    pub fn new(access: A) -> Self {
        Self { access }
    }
}

/// Creates a request with metadata from an old request
fn map_meta<F, T>(from: &Request<F>, inner: T) -> Request<T> {
    let mut to = Request::new(inner);
    for kv in from.metadata().iter() {
        match kv {
            KeyAndValueRef::Ascii(key, value) => {
                to.metadata_mut().insert(key, value.clone());
            }
            KeyAndValueRef::Binary(key, value) => {
                to.metadata_mut().insert_bin(key, value.clone());
            }
        }
    }
    to
}

#[tonic::async_trait]
impl<A> JobService for JobServiceImpl<A>
where
    A: AccessProvider + 'static,
{
    type GetJobOutputRowsStream = ContentStream<GetJobOutputRowsResponse>;

    async fn get_job_output_schema(
        &self,
        req: Request<GetJobOutputSchemaRequest>,
    ) -> Result<Response<GetJobOutputSchemaResponse>, Status> {
        let access = self.access.elevate(&req)?;

        let job_id = req.into_inner().job_id;
        let task = access.into_task(&job_id);
        let arrow_schema = task.get_output_schema().await?;

        Ok(Response::new(GetJobOutputSchemaResponse {
            arrow_schema: Some(arrow_schema),
        }))
    }

    /// Get the query results
    async fn get_job_output_rows(
        &self,
        req: Request<GetJobOutputRowsRequest>,
    ) -> Result<Response<ContentStream<GetJobOutputRowsResponse>>, Status> {
        let access = self.access.elevate(&req)?;

        let job_id = req.into_inner().job_id;
        let task = access.into_task(&job_id);

        let content_stream = task
            .get_output_rows()
            .await?
            .map_ok(|arb| GetJobOutputRowsResponse {
                arrow_record_batch: Some(arb),
            });

        Ok(Response::new(Box::pin(content_stream)))
    }

    /// Starts a job. QueryJob::job_id needs to be empty, otherwise the request will be
    /// rejected.
    async fn insert_job(
        &self,
        req: Request<InsertJobRequest>,
    ) -> Result<Response<InsertJobResponse>, Status> {
        let access = self.access.elevate(&req)?;

        let user_job = req
            .into_inner()
            .job
            .ok_or(Status::invalid_argument("inner job cannot be null"))?;
        let job = access.into_new_task().start(user_job).await?;

        Ok(Response::new(InsertJobResponse { job: Some(job) }))
    }

    /// Get the state of a previously inserted query job.
    async fn get_job(
        &self,
        req: Request<GetJobRequest>,
    ) -> Result<Response<GetJobResponse>, Status> {
        let access = self.access.elevate(&req)?;

        let job_id = req.into_inner().job_id;
        let task = access.into_task(&job_id);
        let job = task.to_proto()?;

        Ok(Response::new(GetJobResponse { job: Some(job) }))
    }

    /// Cancel a previously inserted query job.
    async fn cancel_job(
        &self,
        req: Request<CancelJobRequest>,
    ) -> Result<Response<CancelJobResponse>, Status> {
        let access = self.access.elevate(&req)?;

        let job_id = req.into_inner().job_id;
        let task = access.into_task(&job_id);
        task.cancel().await?;
        let job = task.to_proto()?;

        Ok(Response::new(CancelJobResponse { job: Some(job) }))
    }

    async fn query_job(
        &self,
        req: Request<QueryJobRequest>,
    ) -> Result<Response<QueryJobResponse>, Status> {
        let job = req.get_ref().job.clone();
        let timeout = req.get_ref().timeout;

        if timeout > 60 * 30 {
            return Err(Status::invalid_argument(
                "The maximum timeout is 30 minutes. (1800 seconds)",
            ));
        }

        let insert_job_req = map_meta(&req, InsertJobRequest { job });
        let job_id = self
            .insert_job(insert_job_req)
            .await?
            .into_inner()
            .job
            .ok_or(Status::internal("Job insertion did not return job_id"))?
            .id;

        let mut job_done = false;
        let mut delay_millis = 500;
        let start_time = Instant::now();

        while !job_done {
            if start_time.elapsed() > Duration::from_secs(timeout) {
                let cancel_job_request = map_meta(
                    &req,
                    CancelJobRequest {
                        job_id: job_id.clone(),
                    },
                );
                self.cancel_job(cancel_job_request).await?;
                return Err(Status::deadline_exceeded(format!(
                    "Timeout occurred... Job {} cancelled.",
                    job_id
                )));
            }

            let get_job_req = map_meta(
                &req,
                GetJobRequest {
                    job_id: job_id.clone(),
                },
            );

            let job = self
                .get_job(get_job_req)
                .await?
                .into_inner()
                .job
                .ok_or(Status::internal(format!(
                    "Could not get job for id {}",
                    job_id
                )))?;

            let state = job
                .status
                .ok_or(Status::internal(format!(
                    "Job {} does not have an associated state",
                    job_id
                )))?
                .state;

            match state {
                3 => {
                    job_done = true;
                }
                _ => {
                    // exponential backoff
                    tokio::time::delay_for(Duration::from_millis(delay_millis)).await;
                    delay_millis = (delay_millis as f64 * 1.5) as u64;
                }
            }
        }

        let schema_req = map_meta(
            &req,
            GetJobOutputSchemaRequest {
                job_id: job_id.clone(),
            },
        );
        let arrow_schema = self
            .get_job_output_schema(schema_req)
            .await?
            .into_inner()
            .arrow_schema
            .unwrap();

        let row_req = map_meta(
            &req,
            GetJobOutputRowsRequest {
                job_id: job_id.clone(),
            },
        );
        let arrow_record_batches: Vec<parallax_api::ArrowRecordBatch> = self
            .get_job_output_rows(row_req)
            .await?
            .into_inner()
            .try_collect::<Vec<parallax_api::GetJobOutputRowsResponse>>()
            .await?
            .into_iter()
            .map(|resp| resp.arrow_record_batch.unwrap())
            .collect();

        Ok(Response::new(QueryJobResponse {
            arrow_schema: Some(arrow_schema),
            arrow_record_batches,
        }))
    }
    /// List all jobs
    async fn list_jobs(
        &self,
        req: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsResponse>, Status> {
        let access = self.access.elevate(&req)?;

        let jobs = access
            .list_jobs()?
            .into_iter()
            .map(|job| job.to_proto())
            .collect();

        Ok(Response::new(ListJobsResponse { jobs }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::Rng;

    use crate::node::{
        self, access::AccountAccessProvider, state::tests::read_manifest, Backends, Cluster, Node,
        Peer,
    };
    use tokio::runtime::Runtime;
    use tonic::{
        transport::{Channel, Server},
        Response, Status,
    };

    use num_bigint::BigUint;

    use biscuit::jwa::SignatureAlgorithm;
    use biscuit::jwk::RSAKeyParameters;
    use biscuit::jws::RegisteredHeader;
    use biscuit::jws::{self, Header, Secret};
    use biscuit::{ClaimsSet, Compact, RegisteredClaims};
    use ring::signature::{KeyPair, RsaKeyPair};

    fn public_key_parameters(key_pair: &RsaKeyPair) -> RSAKeyParameters {
        RSAKeyParameters {
            n: BigUint::from_bytes_be(
                key_pair
                    .public_key()
                    .modulus()
                    .big_endian_without_leading_zero(),
            ),
            e: BigUint::from_bytes_be(
                key_pair
                    .public_key()
                    .exponent()
                    .big_endian_without_leading_zero(),
            ),
            ..Default::default()
        }
    }

    type JWT = biscuit::JWT<biscuit::Empty, biscuit::Empty>;

    fn mk_secret() -> Secret {
        Secret::rsa_keypair_from_file("test/private_key.der").unwrap()
    }

    fn mk_token() -> Compact {
        let secret = mk_secret();
        let decoded = JWT::new_decoded(
            Header::from_registered_header(RegisteredHeader {
                algorithm: SignatureAlgorithm::RS256,
                ..Default::default()
            }),
            ClaimsSet {
                registered: RegisteredClaims {
                    issuer: Some("auth@parallax-demo.openquery.io".parse().unwrap()),
                    subject: Some("unit-tester".parse().unwrap()),
                    ..Default::default()
                },
                private: biscuit::Empty {},
            },
        );
        decoded.encode(&secret).unwrap().unwrap_encoded()
    }

    fn spawn_server(rt: &mut Runtime, node: Arc<Node>) -> u16 {
        let rand_port: u16 = rand::thread_rng().gen_range(5000, 8000);
        let access = AccountAccessProvider::new(node);
        rt.spawn(async move {
            Server::builder()
                .add_service(JobServiceServer::new(JobServiceImpl::new(access)))
                .serve(format!("0.0.0.0:{}", rand_port).parse().unwrap())
                .await
                .unwrap()
        });
        rand_port
    }

    fn mk_req<T>(t: T) -> Request<T> {
        let mut req = Request::new(t);
        req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", mk_token().encode()).parse().unwrap(),
        );
        req
    }

    fn test_query_rpc<F, Fut, O>(f: F) -> O
    where
        Fut: std::future::Future<Output = O>,
        F: FnOnce(JobServiceClient<Channel>) -> Fut,
    {
        let mut rt = Runtime::new().unwrap();
        let random_scope = uuid::Uuid::new_v4().to_simple().to_string();
        let access = Arc::new(node::tests::mk_node(&random_scope));

        for resource in read_manifest().into_iter() {
            access.create_resource(resource).unwrap();
        }

        let port = spawn_server(&mut rt, access);

        rt.block_on(async {
            // wait a bit for the server to spawn up
            tokio::time::delay_for(std::time::Duration::from_secs(2)).await;
            let mut client = JobServiceClient::connect(format!("http://localhost:{}", port))
                .await
                .unwrap();
            f(client).await
        })
    }

    #[test]
    fn query_rpc_get_non_existent_job() {
        test_query_rpc(async move |mut client| {
            let req = mk_req(GetJobRequest {
                job_id: "this_is_not_a_valid_id".to_string(),
            });
            match client.get_job(req).await {
                Err(status) => assert_eq!(status.code(), tonic::Code::NotFound),
                Ok(_) => (),
            };
        })
    }

    #[test]
    fn query_rpc_query_job() {
        test_query_rpc(async move |mut client| {
            let job = Job {
                query: "SELECT business_id FROM yelp.business".to_string(),
                ..Default::default()
            };
            let req = mk_req(QueryJobRequest {
                job: Some(job),
                timeout: 600,
            });
            let query_job_resp = client.query_job(req).await.unwrap().into_inner();

            assert!(query_job_resp.arrow_schema.unwrap().serialized_schema.len() > 0);
            assert!(
                query_job_resp
                    .arrow_record_batches
                    .get(0)
                    .unwrap()
                    .serialized_record_batch
                    .len()
                    > 0
            );
        })
    }

    #[test]
    fn query_rpc_query_job_timeout_exceeded() {
        test_query_rpc(async move |mut client| {
            let job = Job {
                query: "SELECT business_id FROM yelp.business".to_string(),
                ..Default::default()
            };
            let req = mk_req(QueryJobRequest {
                job: Some(job),
                timeout: 3601,
            });

            match client.query_job(req).await {
                Ok(_) => panic!("Timeout should be exceeded"),
                Err(status) => assert_eq!(status.code(), tonic::Code::InvalidArgument),
            }
        })
    }

    #[test]
    fn query_rpc_insert_job_and_get_results_dont_exist() {
        test_query_rpc(async move |mut client| {
            let job = Job {
                query: "SELECT absurd FROM i_do_not_exist".to_string(),
                ..Default::default()
            };
            let req = mk_req(InsertJobRequest { job: Some(job) });
            let job = client
                .insert_job(req)
                .await
                .unwrap()
                .into_inner()
                .job
                .unwrap();
            let req = mk_req(GetJobRequest { job_id: job.id });
            let job = client.get_job(req).await.unwrap().into_inner().job.unwrap();

            tokio::time::delay_for(std::time::Duration::from_secs(60)).await;
            let req = mk_req(GetJobRequest { job_id: job.id });
            let job = client.get_job(req).await.unwrap().into_inner().job.unwrap();

            let status = job.status.unwrap();
            assert_eq!(status.state, JobState::Done as i32);
            assert!(status.final_error.is_some());
        })
    }

    #[test]
    fn query_rpc_insert_job_and_list() {
        test_query_rpc(async move |mut client| {
            let job = Job {
                query: "SELECT business_id FROM yelp.business".to_string(),
                ..Default::default()
            };
            let req = mk_req(InsertJobRequest {
                job: Some(job.clone()),
            });
            let job1 = client
                .insert_job(req)
                .await
                .unwrap()
                .into_inner()
                .job
                .unwrap();
            let req = mk_req(InsertJobRequest { job: Some(job) });
            let job2 = client
                .insert_job(req)
                .await
                .unwrap()
                .into_inner()
                .job
                .unwrap();
            let list_req = mk_req(ListJobsRequest {});
            let jobs = client.list_jobs(list_req).await.unwrap().into_inner().jobs;
            assert!(jobs.len() > 1);
        })
    }

    #[test]
    fn query_rpc_insert_job_and_it_completes() {
        let job = insert_job_and_get(
            "SELECT business_id, COUNT(funny) \
             FROM yelp.review \
             GROUP BY business_id",
        );
        println!("{:#?}", job);
        let status = job.status.unwrap();
        assert_eq!(status.state, JobState::Done as i32);
        assert!(status.final_error.is_none());
    }

    #[test]
    fn query_rpc_insert_job_but_it_is_forbidden() {
        let job = insert_job_and_get("SELECT * FROM yelp.business");
        let status = job.status.unwrap();
        assert_eq!(status.state, JobState::Done as i32);
        assert!(status.final_error.is_some());
    }

    fn insert_job_and_get<S: ToString>(query: S) -> Job {
        test_query_rpc(async move |mut client| {
            let job = Job {
                query: query.to_string(),
                ..Default::default()
            };
            let req = mk_req(InsertJobRequest { job: Some(job) });
            let job = client
                .insert_job(req)
                .await
                .unwrap()
                .into_inner()
                .job
                .unwrap();
            let req = mk_req(GetJobRequest { job_id: job.id });
            let job = client.get_job(req).await.unwrap().into_inner().job.unwrap();

            tokio::time::delay_for(std::time::Duration::from_secs(60)).await;
            let req = mk_req(GetJobRequest { job_id: job.id });
            let job = client.get_job(req).await.unwrap().into_inner().job.unwrap();
            job
        })
    }
}
