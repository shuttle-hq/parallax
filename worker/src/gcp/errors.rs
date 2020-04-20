#[derive(Debug)]
pub enum GcpError {
    HyperError(hyper::error::Error),
    HttpError(http::Error),
    InvalidUri(http::uri::InvalidUri),
    SerdeError(serde_json::Error),
    ApiError(String),
    InvalidResponse(String),
    JobBuilderError(String),
    AuthError(String),
}

impl std::fmt::Display for GcpError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::HyperError(err) => err.fmt(f),
            Self::HttpError(err) => err.fmt(f),
            Self::InvalidUri(err) => err.fmt(f),
            Self::SerdeError(err) => err.fmt(f),
            Self::ApiError(err) => {
                write!(f, "a GCP API call returned the following error: '{}'", err)
            }
            Self::InvalidResponse(resp) => {
                write!(f, "a GCP API call returned an invalid response: '{}'", resp)
            }
            Self::JobBuilderError(err) => {
                write!(f, "a deferred job builder failed to build with '{}'", err)
            }
            Self::AuthError(err) => write!(f, "there was an error in the GCP auth flow: '{}'", err),
        }
    }
}

impl From<hyper::error::Error> for GcpError {
    fn from(err: hyper::error::Error) -> GcpError {
        Self::HyperError(err)
    }
}

impl From<http::Error> for GcpError {
    fn from(err: http::Error) -> GcpError {
        Self::HttpError(err)
    }
}

impl From<serde_json::Error> for GcpError {
    fn from(err: serde_json::Error) -> GcpError {
        Self::SerdeError(err)
    }
}

impl From<http::uri::InvalidUri> for GcpError {
    fn from(err: http::uri::InvalidUri) -> GcpError {
        Self::InvalidUri(err)
    }
}

impl std::error::Error for GcpError {}

pub type Result<T> = std::result::Result<T, GcpError>;
