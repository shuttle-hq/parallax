pub use serde::{Deserialize, Serialize};

pub use std::cmp::{self, max};
pub use std::collections::{HashMap, HashSet};
pub use std::convert::{TryFrom, TryInto};
pub use std::io;
pub use std::iter::FromIterator;
pub use std::iter::IntoIterator;
pub use std::marker::PhantomData;
pub use std::ops::Deref;
pub use std::path::Path;
pub use std::pin::Pin;
pub use std::str::FromStr;
pub use std::sync::{Arc, Mutex};

pub use chrono::{DateTime, Utc};

pub use async_trait::async_trait;

pub use parallax_api::*;

pub use jac::prelude::*;

pub use entish::prelude::*;

pub use uuid::Uuid;

pub use tonic::{metadata::MetadataMap, Request, Response, Status, Streaming};

pub use futures::future::TryFutureExt;
pub use futures::future::{join_all, FutureExt};
pub use futures::prelude::{Future, Stream, TryFuture, TryStream};
pub use futures::stream::TryStreamExt;

pub type ContentStream<T> = Pin<
    Box<dyn TryStream<Ok = T, Error = Status, Item = Result<T, Status>> + Send + Sync + 'static>,
>;
