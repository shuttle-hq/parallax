use super::super::parallax::r#type::error::v1 as error_v1;
use error_v1::error::Details;
use error_v1::*;

use tonic::Status;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "reason: {}", self.reason)?;
        if let Some(details) = self.details.as_ref() {
            write!(f, " (details: {:?})", details)?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {}

impl Error {
    pub fn new_detailed<S, D>(reason: S, details: D) -> Self
    where
        S: AsRef<str>,
        D: Into<Details>,
    {
        Self {
            reason: reason.as_ref().to_string(),
            details: Some(details.into()),
        }
    }
    pub fn new<S: AsRef<str>>(reason: S) -> Self {
        Self {
            reason: reason.as_ref().to_string(),
            ..Default::default()
        }
    }
}

pub use access_error::AccessErrorKind;

impl std::fmt::Display for AccessErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Forbidden => write!(f, "Forbidden"),
            Self::BadRequest => write!(f, "BadRequest"),
            Self::Unavailable => write!(f, "Unavailable"),
            Self::NotFound => write!(f, "NotFound"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

impl From<Error> for Status {
    fn from(error: Error) -> Status {
        if let Some(details) = error.details.clone() {
            match details {
                Details::Access(access_error) => (*access_error).into(),
                _ => Status::unknown(error.to_string()),
            }
        } else {
            Status::unknown(error.to_string())
        }
    }
}

impl From<AccessError> for Status {
    fn from(access_error: AccessError) -> Status {
        let cause = access_error
            .cause
            .map(|cause| format!("(caused by: {})", cause))
            .unwrap_or_default();
        let desc = format!("{} {}", access_error.description, cause);
        let kind = AccessErrorKind::from_i32(access_error.kind).unwrap_or_default();
        match kind {
            AccessErrorKind::Forbidden => Status::permission_denied(desc),
            AccessErrorKind::BadRequest => Status::unauthenticated(desc),
            AccessErrorKind::Unavailable => Status::unavailable(desc),
            AccessErrorKind::NotFound => Status::not_found(desc),
            _ => Status::unknown(desc),
        }
    }
}

impl From<AccessError> for Error {
    fn from(err: AccessError) -> Self {
        Self::new_detailed(
            "an error was triggered by a user action",
            Details::Access(Box::new(err)),
        )
    }
}

impl AccessError {
    pub fn wrap(err: Error) -> Self {
        let mut def = Self::default();
        def.cause = Some(Box::new(err));
        def
    }
}

impl From<AccessErrorKind> for AccessError {
    fn from(kind: AccessErrorKind) -> Self {
        Self {
            kind: kind as i32,
            ..Default::default()
        }
    }
}

impl std::fmt::Display for AccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let desc = format!("({})", self.description);
        let kind = AccessErrorKind::from_i32(self.kind).unwrap_or_default();
        let culprit = format!("[{}]", self.culprit);
        write!(f, "AccessError{}::{}{}", culprit, kind, desc)
    }
}

impl std::error::Error for AccessError {}

impl From<swamp::TypeError> for ScopeError {
    fn from(err: swamp::TypeError) -> Self {
        match err {
            swamp::TypeError::Missing(field) => Self {
                kind: ScopeErrorKind::BadObject as i32,
                source: field.to_string(),
                description: "field is missing".to_string(),
            },
            swamp::TypeError::Invalid(s) => Self {
                kind: ScopeErrorKind::InvalidType as i32,
                source: s,
                description: "not a valid type name".to_string(),
            },
        }
    }
}

impl From<swamp::TypeError> for Error {
    fn from(err: swamp::TypeError) -> Self {
        ScopeError::from(err).into()
    }
}

pub use scope_error::ScopeErrorKind;

impl std::fmt::Display for ScopeErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::Redis => write!(f, "Redis"),
            Self::NotFound => write!(f, "NotFound"),
            Self::AlreadyExists => write!(f, "AlreadyExists"),
            Self::InvalidType => write!(f, "InvalidType"),
            Self::BadObject => write!(f, "BadObject"),
            Self::Mismatch => write!(f, "Mismatch"),
            Self::BadSplat => write!(f, "BadSplat"),
            Self::Changed => write!(f, "Changed"),
        }
    }
}

impl ScopeError {
    pub fn not_found<S: ToString + ?Sized>(source: &S) -> Self {
        Self {
            kind: ScopeErrorKind::NotFound as i32,
            source: source.to_string(),
            ..Default::default()
        }
    }

    pub fn already_exists<S: ToString + ?Sized>(source: &S) -> Self {
        Self {
            kind: ScopeErrorKind::AlreadyExists as i32,
            source: source.to_string(),
            ..Default::default()
        }
    }
}

impl std::fmt::Display for ScopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = ScopeErrorKind::from_i32(self.kind).unwrap_or_default();
        write!(
            f,
            "ScopeError::{}({}: {})",
            kind, self.source, self.description
        )
    }
}

impl std::error::Error for ScopeError {}

impl From<ScopeError> for Error {
    fn from(err: ScopeError) -> Self {
        Self::new_detailed("an error around a scope of resources", err)
    }
}

impl From<jac::redis::Error> for ScopeError {
    fn from(err: jac::redis::Error) -> Self {
        let kind = ScopeErrorKind::Redis;
        let description = err.to_string();
        Self {
            kind: kind as i32,
            description,
            ..Default::default()
        }
    }
}

impl From<jac::redis::Error> for Error {
    fn from(err: jac::redis::Error) -> Self {
        ScopeError::from(err).into()
    }
}

impl std::fmt::Display for ValidateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ValidateError({}: {})", self.r#where, self.description)
    }
}

impl std::error::Error for ValidateError {}

impl From<ValidateError> for Error {
    fn from(err: ValidateError) -> Self {
        Self::new_detailed("a query or attached resource failed to validate", err)
    }
}

pub use backend_error::BackendErrorKind;

impl std::fmt::Display for BackendErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "Unknown"),
            Self::Io => write!(f, "Io"),
            Self::Unavailable => write!(f, "Unavailable"),
            Self::Missing => write!(f, "Missing"),
        }
    }
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = BackendErrorKind::from_i32(self.kind).unwrap_or_default();
        write!(
            f,
            "BackendError::{}({}: {})",
            kind, self.source, self.description
        )
    }
}

impl std::error::Error for BackendError {}

impl From<std::io::Error> for BackendError {
    fn from(err: std::io::Error) -> Self {
        let source = <std::io::Error as std::error::Error>::source(&err)
            .map(|s| s.to_string())
            .unwrap_or_default();
        Self {
            kind: BackendErrorKind::Io as i32,
            source,
            description: err.to_string(),
        }
    }
}

impl From<BackendError> for Error {
    fn from(err: BackendError) -> Self {
        Self::new_detailed(
            "an error occurred dealing with one of the backends involved in this job",
            err,
        )
    }
}
