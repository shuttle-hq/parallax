use crate::common::*;

const MAX_TIMEOUT_MS: u64 = 3600; // FIXME: expose as a state setting

use crate::node::{Access, AccessProvider};

pub struct JobServiceImpl<A> {
    access: A,
}

impl<A> JobServiceImpl<A> {
    pub fn new(access: A) -> Self {
        Self { access }
    }
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

    type JWT = biscuit::JWT<biscuit::Empty, jws::Header<biscuit::Empty>>;

    fn mk_key_pair() -> (RsaKeyPair, RSAKeyParameters) {
        let key_pair = node::tests::mk_key_pair();
        let public = public_key_parameters(&key_pair);
        (key_pair, public)
    }

    fn print_public_key() {
        let encoded = base64::encode(&serde_json::to_string(&mk_key_pair().1).unwrap());
        println!("{}", encoded)
    }

    fn mk_token() -> Compact {
        let key_pair = mk_key_pair().0;
        let secret = Secret::RsaKeyPair(Arc::new(key_pair));
        let decoded = JWT::new_decoded(
            Header {
                registered: RegisteredHeader {
                    algorithm: SignatureAlgorithm::RS256,
                    ..Default::default()
                },
                private: Default::default(),
            },
            ClaimsSet {
                registered: RegisteredClaims {
                    issuer: Some("auth@parallax-demo.openquery.io".parse().unwrap()),
                    subject: Some("unit-tester".parse().unwrap()),
                    ..Default::default()
                },
                private: Default::default(),
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

            tokio::time::delay_for(std::time::Duration::from_secs(10)).await;
            let req = mk_req(GetJobRequest { job_id: job.id });
            let job = client.get_job(req).await.unwrap().into_inner().job.unwrap();

            let status = job.status.unwrap();
            assert_eq!(status.state, JobState::Done as i32);
            assert!(status.final_error.is_some());
        })
    }

    #[test]
    fn query_rpc_insert_job_and_it_completes() {
        let job = insert_job_and_get("SELECT business_id FROM yelp.business");
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

            tokio::time::delay_for(std::time::Duration::from_secs(10)).await;
            let req = mk_req(GetJobRequest { job_id: job.id });
            let job = client.get_job(req).await.unwrap().into_inner().job.unwrap();
            job
        })
    }
}
