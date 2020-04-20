use crate::common::*;

use super::*;

/// An AccessProvider giving unconditional access to its underlying
/// with no identity validation or permission checking
#[derive(Clone)]
pub struct BootstrapAccessProvider<A> {
    inner: A,
}

impl<A> BootstrapAccessProvider<A>
where
    A: Access + Clone,
{
    pub fn new(inner: A) -> Self {
        Self { inner }
    }
}

impl<A> AccessProvider for BootstrapAccessProvider<A>
where
    A: Access + Clone,
{
    type Access = A;
    fn elevate<R>(&self, req: &Request<R>) -> AccessResult<Self::Access> {
        error!("elevated a claim blindly");
        Ok(self.inner.clone())
    }
}
