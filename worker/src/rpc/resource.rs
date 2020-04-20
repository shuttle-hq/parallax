use crate::common::*;
use crate::node::{Access, AccessProvider};

pub struct ResourceServiceImpl<A> {
    access: A,
}

impl<A> ResourceServiceImpl<A> {
    pub fn new(access: A) -> Self {
        Self { access }
    }
}

#[tonic::async_trait]
impl<A> ResourceService for ResourceServiceImpl<A>
where
    A: AccessProvider + 'static,
{
    async fn list_resources(
        &self,
        req: Request<ListResourcesRequest>,
    ) -> Result<Response<ListResourcesResponse>, Status> {
        let access = self.access.elevate(&req)?;

        let pat = req
            .into_inner()
            .pattern
            .parse()
            .map_err(|e| Status::invalid_argument(format!("could not parse pattern: {}", e)))?;
        let resources = access.resources(&pat)?;
        Ok(Response::new(ListResourcesResponse { resources }))
    }

    async fn acquire_lock(
        &self,
        req: Request<AcquireLockRequest>,
    ) -> Result<Response<AcquireLockResponse>, Status> {
        let access = self.access.elevate(&req)?;
        let lock_id = access.acquire_lock()?;
        Ok(Response::new(AcquireLockResponse { lock_id }))
    }

    async fn release_lock(
        &self,
        req: Request<ReleaseLockRequest>,
    ) -> Result<Response<ReleaseLockResponse>, Status> {
        let access = self.access.elevate(&req)?;
        access.release_lock(&req.into_inner().lock_id)?;
        Ok(Response::new(ReleaseLockResponse {}))
    }

    async fn create_resource(
        &self,
        req: Request<CreateResourceRequest>,
    ) -> Result<Response<CreateResourceResponse>, Status> {
        let access = self.access.elevate(&req)?;
        let resource = req
            .into_inner()
            .resource
            .ok_or(Status::invalid_argument("inner resource cannot be null"))?;

        let resource = access.create_resource(resource).map_err(|e| {
            if let Some(Details::Scope(scope_err)) = e.details.as_ref() {
                let kind = ScopeErrorKind::from_i32(scope_err.kind).unwrap_or_default();
                if ScopeErrorKind::AlreadyExists == kind {
                    return Status::already_exists(scope_err.source.to_string());
                }
            }
            e.into()
        })?;

        let resource_name = resource
            .block_type()
            .map_err(|_| Status::internal("invalid block type after evaluation"))?
            .to_string();

        Ok(Response::new(CreateResourceResponse {
            resource_name,
            resource: Some(resource),
        }))
    }

    async fn update_resource(
        &self,
        req: Request<UpdateResourceRequest>,
    ) -> Result<Response<UpdateResourceResponse>, Status> {
        let access = self.access.elevate(&req)?;
        let req = req.into_inner();

        let resource = req
            .resource
            .ok_or(Status::invalid_argument("inner resource cannot be null"))?;
        let resource_name = req
            .resource_name
            .parse()
            .map_err(|_| Status::invalid_argument("could not parse resource_name"))?;

        let resource = access
            .update_resource(&resource_name, resource)
            .map_err(|e| {
                if let Some(Details::Scope(scope_err)) = e.details.as_ref() {
                    let kind = ScopeErrorKind::from_i32(scope_err.kind).unwrap_or_default();
                    if kind == ScopeErrorKind::NotFound {
                        return Status::not_found(scope_err.source.to_string());
                    }
                }
                e.into()
            })?;

        let resource_name = resource
            .block_type()
            .map_err(|_| Status::internal("invalid block type"))?
            .to_string();

        Ok(Response::new(UpdateResourceResponse {
            resource_name,
            resource: Some(resource),
        }))
    }

    async fn delete_resource(
        &self,
        req: Request<DeleteResourceRequest>,
    ) -> Result<Response<DeleteResourceResponse>, Status> {
        let access = self.access.elevate(&req)?;
        let resource_name = req
            .into_inner()
            .resource_name
            .parse()
            .map_err(|_| Status::invalid_argument("could not parse resource_name"))?;

        access.delete_resource(&resource_name).map_err(|e| {
            if let Some(Details::Scope(scope_error)) = e.details.as_ref() {
                let kind = ScopeErrorKind::from_i32(scope_error.kind).unwrap_or_default();
                if kind == ScopeErrorKind::NotFound {
                    return Status::not_found(scope_error.source.to_string());
                }
            }
            e.into()
        })?;

        Ok(Response::new(DeleteResourceResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{access::BootstrapAccessProvider, tests::mk_node};

    use tonic::{
        transport::{channel::Channel, Server},
        Code, Response, Status,
    };

    use rand::{
        distributions::{Range, Sample},
        Rng,
    };

    use tokio::runtime::Runtime;

    fn test_resource<F, Fut, O>(f: F) -> O
    where
        F: FnOnce(ResourceServiceClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = O>,
    {
        let mut rt = Runtime::new().unwrap();
        let mut port_range = Range::new(6000, 12000);
        let mut rng = rand::thread_rng();
        let random_port = port_range.sample(&mut rng);
        let random_scope = uuid::Uuid::new_v4().to_simple().to_string();
        let access = BootstrapAccessProvider::new(Arc::new(mk_node(&random_scope)));
        rt.spawn(async move {
            Server::builder()
                .add_service(ResourceServiceServer::new(ResourceServiceImpl::new(access)))
                .serve(format!("127.0.0.1:{}", random_port).parse().unwrap())
                .await
                .unwrap();
        });

        rt.block_on(async move {
            // wait a bit for the server to spawn up
            tokio::time::delay_for(std::time::Duration::from_secs(2)).await;
            let uri = format!("http://127.0.0.1:{}", random_port);
            let client = ResourceServiceClient::connect(uri).await.unwrap();
            f(client).await
        })
    }

    fn mk_test_user(email: &str) -> User {
        User {
            name: "user0".to_string(),
            email: email.to_string(),
            public_keys: vec!["AKEY".to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn governor_create_resource() {
        test_resource(async move |mut client| {
            let resource = Resource {
                resource: Some(ResourceEnum::User(mk_test_user("user0@company.co"))),
            };

            let req = Request::new(CreateResourceRequest {
                resource: Some(resource.clone()),
            });
            client.create_resource(req).await.unwrap();

            let req = Request::new(CreateResourceRequest {
                resource: Some(resource.clone()),
            });
            match client.create_resource(req).await {
                Err(err) => assert_eq!(err.code(), Code::AlreadyExists),
                Ok(_) => panic!("create twice not allowed"),
            }
        })
    }

    #[test]
    fn governor_list_resources() {
        test_resource(async move |mut client| {
            let resource = Resource {
                resource: Some(ResourceEnum::User(mk_test_user("user0@company.co"))),
            };

            let req = Request::new(CreateResourceRequest {
                resource: Some(resource.clone()),
            });

            client.create_resource(req).await.unwrap();

            let req = Request::new(ListResourcesRequest {
                pattern: "*".to_string(),
            });
            let ListResourcesResponse { mut resources } =
                client.list_resources(req).await.unwrap().into_inner();

            assert_eq!(resources.len(), 1);
            assert_eq!(resources.pop().unwrap(), resource)
        })
    }

    #[test]
    fn governor_update_resource() {
        test_resource(async move |mut client| {
            let resource = Resource {
                resource: Some(ResourceEnum::User(mk_test_user("user0@company.co"))),
            };

            let req = Request::new(CreateResourceRequest {
                resource: Some(resource),
            });
            client.create_resource(req).await.unwrap();

            let modified_resource = Resource {
                resource: Some(ResourceEnum::User(mk_test_user("user0@new_email.co"))),
            };
            let req = Request::new(UpdateResourceRequest {
                resource_name: modified_resource.block_type().unwrap().to_string(),
                resource: Some(modified_resource.clone()),
            });
            client.update_resource(req).await.unwrap();

            let req = Request::new(ListResourcesRequest {
                pattern: "resource.user.user0".to_string(),
            });
            let resources = client.list_resources(req).await.unwrap().into_inner();
            assert_eq!(resources.resources[0], modified_resource);
        })
    }

    #[test]
    fn governor_delete_resource() {
        test_resource(async move |mut client| {
            let resource = Resource {
                resource: Some(ResourceEnum::User(mk_test_user("user0@company.co"))),
            };

            let req = Request::new(DeleteResourceRequest {
                resource_name: resource.block_type().unwrap().to_string(),
            });
            match client.delete_resource(req).await {
                Err(err) => assert_eq!(err.code(), Code::NotFound),
                _ => panic!("deleted when not exists"),
            };

            let req = Request::new(CreateResourceRequest {
                resource: Some(resource.clone()),
            });
            client.create_resource(req).await.unwrap();

            let req = Request::new(DeleteResourceRequest {
                resource_name: resource.block_type().unwrap().to_string(),
            });
            client.delete_resource(req).await.unwrap();
        })
    }
}
