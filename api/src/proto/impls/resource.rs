//! Most of the impls here should really be under swamp but for the sake of simplicity
//! they ended up here for now.
use super::super::parallax::config::resource::v1 as resource_v1;

use super::super::parallax::r#type::error::v1 as error_v1;
use error_v1::{scope_error::ScopeErrorKind, ScopeError};

use crate::TryUnwrap;

macro_rules! impl_try_unwrap {
    ($up:ident.$ty:ident as $fi:ident) => {
        impl TryUnwrap for $up {
            type Into = $ty;
            fn try_unwrap(self) -> std::result::Result<Self::Into, ScopeError> {
                let err = stringify!($ty);
                let up = stringify!($up);
                let fi = stringify!($fi);
                self.$fi.ok_or(ScopeError {
                    kind: ScopeErrorKind::BadObject as i32,
                    source: up.to_string(),
                    description: format!("{}::{} should be Some({}) but is None", up, fi, err),
                })
            }
        }
    };
}

macro_rules! impl_try_into {
    ($up:ident.$ty:ident as $fi:ident -> { $($var_ty:ident,)* }) => {
        $(
            impl std::convert::TryInto<$var_ty> for $up {
                type Error = ScopeError;
                fn try_into(self) -> std::result::Result<$var_ty, Self::Error> {
                    match self.try_unwrap()? {
                        $ty::$var_ty(variant) => Ok(variant),
                        _ => {
                            let var_ty = stringify!($var_ty);
                            let up = stringify!($up);
                            Err(ScopeError {
                                kind: ScopeErrorKind::Mismatch as i32,
                                source: up.to_string(),
                                description: format!("expected {} but found something else", var_ty)
                            })
                        }
                    }
                }
            }
        )*

            impl_try_unwrap!($up.$ty as $fi);

        impl $up {
            pub fn try_downcast<T>(self) ->
                std::result::Result<T, <Self as std::convert::TryInto<T>>::Error>
            where
                Self: std::convert::TryInto<T>
            {
                <Self as std::convert::TryInto<T>>::try_into(self)
            }
        }
    };
}

use resource_v1::{
    backend::Backend as BackendEnum, data::Data as DataEnum, policy::Policy as PolicyEnum,
    resource::Resource as ResourceEnum, Backend, BigQueryBackend as BigQuery,
    CollectionData as Collection, Data, Dataset, DrillBackend as Drill, Group, HashPolicy as Hash,
    MongoBackend as Mongo, ObfuscatePolicy as Obfuscate, Policy, Resource, TableData as Table,
    User, WhitelistPolicy as Whitelist,
};

impl_try_into!(Resource.ResourceEnum as resource -> { Backend, User, Group, Dataset, });

impl_try_into!(Backend.BackendEnum as backend -> { Mongo, Drill, BigQuery, });

impl_try_into!(Data.DataEnum as data -> { Table, Collection, });

impl_try_into!(Policy.PolicyEnum as policy -> { Whitelist, Hash, Obfuscate, });
