use crate::common::{ValidateError as ApiValidateError, *};

use super::ContextKey;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ValidateError {
    InvalidSql(String),
    //InferenceError(crate::query::oql::InferError),
    //OptimizationError(crate::query::optimize::OptimizeError),
    SerdeError(String),
    MultipleStatementsNotAllowed,
    EmptyRequest,
    NotAQuery(String),
    UnknownFunction(String),
    Wip(String),
    TableNotFound(ContextKey),
    AmbiguousTableName(ContextKey),
    RelationNotFound(String),
    SchemaNotFound(String, String, String),
    InvalidTableName(String),
    ColumnNotFound(String),
    EmptyIdent,
    AmbiguousColumnName(String),
    Expected(String),
    OverlappingAlias(String),
    UnexpectedWildcard,
    InvalidNumberLiteral(String),
    Internal(String),
    MissingIdentifier(String),
    InvalidIdentifier(String),
    InvalidLiteral(String),
    InvalidFunctionName(String),
    InvalidType(String, String),
    SchemaMismatch(String),
    NotSupported(String),
    UnknownType(String),
    Insufficient(String),
}

impl From<sqlparser::parser::ParserError> for ValidateError {
    fn from(err: sqlparser::parser::ParserError) -> ValidateError {
        Self::InvalidSql(err.to_string())
    }
}

//impl From<crate::query::oql::InferError> for ValidateError {
//    fn from(err: crate::query::oql::InferError) -> ValidateError {
//        Self::InferenceError(err)
//    }
//}

impl From<serde_json::Error> for ValidateError {
    fn from(err: serde_json::Error) -> ValidateError {
        Self::SerdeError(format!("{:?}", err))
    }
}

//impl From<crate::query::optimize::OptimizeError> for ValidateError {
//    fn from(err: crate::query::optimize::OptimizeError) -> ValidateError {
//        Self::OptimizationError(err)
//    }
//}

impl std::fmt::Display for ValidateError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::InvalidSql(source) => write!(f, "sqlparser error: {}", source),
            //Self::InferenceError(source) =>
            //    write!(f, "inference error: {}", source),
            //Self::OptimizationError(source) =>
            //    write!(f, "optimization error: {}", source),
            Self::SerdeError(source) => write!(f, "serde error: {}", source),
            Self::MultipleStatementsNotAllowed => write!(
                f,
                "SQL requests cannot contain multiple sequenced statements"
            ),
            Self::EmptyRequest => write!(f, "SQL request does not contain a valid statement"),
            Self::NotAQuery(stmt) => write!(f, "SQL statement is not a query: '{}'", stmt),
            Self::UnknownFunction(f_n) => write!(f, "unknown function named: '{}'", f_n),
            Self::Wip(feat) => write!(
                f,
                "requires a feature that is not implemented yet: '{}'",
                feat
            ),
            Self::TableNotFound(k) => write!(
                f,
                "the requested table {} was not found in the current context",
                k
            ),
            Self::AmbiguousTableName(k) => write!(f, "the request table named {} is ambiguous", k),
            Self::RelationNotFound(rid) => {
                write!(f, "the requested relation '{}' was not found", rid)
            }
            Self::SchemaNotFound(p, d, t) => write!(
                f,
                "the schema for the table '{}.{}.{}' was not found \
                     (this is not supposed to happen)",
                p, d, t
            ),
            Self::InvalidTableName(on) => {
                write!(f, "the object '{}' is not a valid table name", on)
            }
            Self::ColumnNotFound(cn) => {
                write!(f, "the requested column named '{}' was not found", cn)
            }
            Self::EmptyIdent => write!(f, "an empty identifier was used as a column name"),
            Self::AmbiguousColumnName(cn) => write!(
                f,
                "the column name '{}' is ambiguous in the current context",
                cn
            ),
            Self::OverlappingAlias(alias) => {
                write!(f, "the alias '{}' is used more than once", alias)
            }
            Self::InvalidNumberLiteral(nl) => write!(
                f,
                "the wrapped up literal '{}' is not a valid number literal \
                     (this is not supposed to happen)",
                nl
            ),
            Self::UnexpectedWildcard => write!(
                f,
                "wildcards are only allowed in unaliased top-level expressions \
                           or as the only argument of an unaliased top-level function"
            ),
            Self::Expected(s) => write!(f, "expected {}", s),
            Self::Internal(err) => write!(
                f,
                "something went wrong with this and that is all we know: {}",
                err
            ),
            Self::MissingIdentifier(id) => write!(
                f,
                "the table named {} does not have an assigned identifier",
                id
            ),
            Self::InvalidIdentifier(id) => write!(f, "invalid identifier: {}", id),
            Self::InvalidLiteral(id) => write!(f, "invalid literal: {}", id),
            Self::InvalidFunctionName(s) => write!(f, "invalid function name: {}", s),
            Self::InvalidType(s, p) => {
                write!(f, "invalid type: {} is needed but {} was used", s, p)
            }
            Self::SchemaMismatch(c) => write!(
                f,
                "schema for column `{}` is not the same in all operands",
                c
            ),
            Self::NotSupported(feat) => write!(f, "feature not supported: {}", feat),
            Self::UnknownType(ty) => write!(f, "unknown type: {}", ty),
            Self::Insufficient(why) => write!(f, "failed due to missing information: {}", why),
        }
    }
}

impl std::error::Error for ValidateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

pub type ValidateResult<T> = std::result::Result<T, ValidateError>;

impl ValidateError {
    pub fn into_error(self) -> ApiValidateError {
        ApiValidateError {
            description: self.to_string(),
            ..Default::default()
        }
    }
}
