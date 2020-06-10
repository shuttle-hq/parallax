use crate::common::*;

pub mod job;
pub use job::JobServiceImpl;

pub mod resource;
pub use resource::ResourceServiceImpl;

pub mod catalog;

fn request_with_metadata<T>(t: T, metadata: &MetadataMap) -> Request<T> {
    let mut req = Request::new(t);
    *req.metadata_mut() = metadata.clone();
    req
}
