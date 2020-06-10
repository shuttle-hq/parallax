//! # Query parsing, plan optimization, high-level composition logic

use crate::common::*;

macro_rules! variant_to_ansatz {
    ($ansatz:ident, #[$_:meta] $id:ident<$($param:ident,)*> $f:tt => $d:tt) => {
        impl<$($($param: ToAnsatz<Ansatz = ExprAnsatz>,)*)*> ToAnsatz for $id<$($($param,)*)*> {
            type Ansatz = $ansatz;
            fn to_ansatz(self) -> Result<Self::Ansatz, CompositionError> {
                let $id$f = self;
                Ok(($d).into())
            }
        }
    };
    ($ansatz:ident, $id:ident<$($param:ident,)*> $f:tt => $d:tt) => {
        impl<$($param: ToAnsatz<Ansatz = ExprAnsatz>,)*> ToAnsatz for $id<$($param,)* $ansatz> {
            type Ansatz = $ansatz;
            fn to_ansatz(self) -> Result<Self::Ansatz, CompositionError> {
                let $id $f = self;
                Ok(($d).into())
            }
        }
    }
}

macro_rules! to_ansatz {
    (
        match<$($param:ident,)*> $up:ident<$ansatz:ident> {
            $($(#[$_:meta])* $id:ident<$($variant_param:ident,)*> $f:tt => $d:tt,)*
        }
    ) => {
        impl<$($param: ToAnsatz<Ansatz = ExprAnsatz>,)*> ToAnsatz for $up<$($param,)* $ansatz> {
            type Ansatz = $ansatz;
            fn to_ansatz(self) -> Result<Self::Ansatz, CompositionError> {
                match self {
                    $(Self::$id(v) => v.to_ansatz(),)*
                    _ => Err(CompositionError::Unimplemented)
                }
            }
        }
        $( variant_to_ansatz! { $ansatz, $(#[$_])* $id<$($variant_param,)*> $f => $d } )*
    }
}

macro_rules! auto_repr {
    (
        $tr:path,
        $node:path,
        {
            #[derive($($attr:path,)*)]
            $vis:vis struct $id:ident {
                $($field_vis:vis $n:ident: $f:path,)*
            }
        }
    ) => {
        #[derive($($attr,)*)]
        $vis struct $id {
            $($field_vis $n: $f,)*
        }

        impl $tr for $id {
            fn dot(node: $node) -> ValidateResult<Self> {
                Ok(Self { $($n: <$f as $tr>::dot(node.map(&mut |n| &n.$n))?,)* })
            }
        }
    }
}

macro_rules! derive_expr_repr {
    ($($it:tt)*) => {
        auto_repr! { ExprRepr, Expr<&Self>, { $($it)* } }
    }
}

macro_rules! derive_rel_repr {
    (over $expr:ident derive $($it:tt)*) => {
        auto_repr! { RelRepr<$expr>, GenericRel<&$expr, &Self>, { $($it)* } }
    }
}

macro_rules! copy_ast_enum {
    ( $(#[derive($($attr:tt,)*)])*
      pub enum $left:path as $right:ident {
          $($variant:ident,)*
      }
    ) => {
        $(#[derive($($attr,)*)])*
        pub enum $right {
            $($variant,)*
        }

        impl<'a> From<&'a $left> for $right {
            fn from(left: &'a $left) -> Self {
                match left {
                    $(<$left>::$variant => Self::$variant,)*
                }
            }
        }

        impl Into<$left> for $right {
            fn into(self) -> $left {
                match self {
                    $(Self::$variant => <$left>::$variant,)*
                }
            }
        }
    };
}

macro_rules! map_variants {
    ($expr:tt as $root:ident {
        $(
            $variant:ident => {
                $($field:ident: $new_field:block,)*
            },
        )*
        $(
            #[unnamed] $unnamed_variant:ident => {
                $($unnamed_field:ident: $new_unnamed_field:block,)*
            },
        )*
        $(
            _ => $catchall:block,
        )*
    }) => {
        match $expr {
            $(
                $root::$variant($variant { $($field,)* }) => {
                    $root::$variant($variant {
                        $(
                            $field: $new_field,
                        )*
                    })
                },
            )*
            $(
                $root::$unnamed_variant($unnamed_variant($($unnamed_field,)*)) => {
                    $root::$unnamed_variant($unnamed_variant($($new_unnamed_field,)*))
                },
            )*
            $(
                _ => $catchall,
            )*
        }
    };
}

macro_rules! rebase_closure {
    ($tree:expr => $from:ident -> $to:ident $closure:block) => {
        async move {
            let rebase_closure = crate::opt::RebaseClosure::<$from, _, _, $to>::new($closure);
            rebase_closure.rebase(&$tree).await
        }
    };
}

/// going from sql string to RelT
pub mod validate;
pub use validate::{ValidateExpr, Validator};

/// errors occuring at parsing, optimization and composition
pub mod error;
pub use error::{ValidateError, ValidateResult};

pub mod meta;
pub use crate::opt::meta::{AudienceBoard, DataType, ExprRepr, Mode, Named, RelRepr, Taint};

pub mod ansatz;
pub use ansatz::{CompositionError, ExprAnsatz, RelAnsatz, ToAnsatz};

pub mod plan;

pub mod transform;
pub use transform::{Policy, PolicyBinding, RelTransformer, Transformed};

pub mod rel;
pub use rel::*;

pub mod expr;
pub use expr::*;

pub mod privacy;
pub use privacy::*;

/// A key for something in a given context. This is basically a wrapper around
/// `str::split(".")`.
#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ContextKey(Vec<String>);

impl ContextKey {
    pub fn with_name(name: &str) -> Self {
        Self(vec![name.to_string()])
    }
    pub fn name(&self) -> &str {
        self.0.get(0).unwrap()
    }
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|s| s.as_str())
    }
    pub fn prefix(&self) -> impl Iterator<Item = &str> {
        let mut iter = self.0.iter().map(|s| s.as_str());
        iter.next();
        iter
    }
    pub fn root(&self) -> Option<&str> {
        // TODO Shouldn't this be 0?
        if self.0.len() == 1 {
            None
        } else {
            self.0.last().map(|s| s.as_str())
        }
    }
    pub fn at_depth(&self, index: usize) -> Option<&str> {
        if self.0.len() < index {
            None
        } else {
            self.0.get(index).map(|s| s.as_str())
        }
    }
    pub fn and_prefix(mut self, prefix: &str) -> Self {
        self.0.push(prefix.to_string());
        self
    }
    pub fn matches(&self, other: &Self) -> bool {
        self.0
            .iter()
            .zip(other.0.iter())
            .all(|(m, o)| o == "*" || m == o)
    }
    pub fn prefix_matches(&self, other: &Self) -> bool {
        self.0[1..] == other.0[1..]
    }
    pub fn inv_matches(&self, other: &Self) -> bool {
        self.0
            .iter()
            .rev()
            .zip(other.0.iter().rev())
            .all(|(m, o)| o == "*" || m == o)
    }
    pub fn remove_common(mut self, other: &Self) -> Option<Self> {
        for o in other.iter() {
            let next = self.0.pop()?;
            if next != *o {
                self.0.push(next);
                break;
            }
        }
        Some(self)
    }
    pub fn with_prefix(&self, prefix: &str) -> Self {
        let mut ids = self.0.clone();
        if ids.len() == 1 {
            ids.push(prefix.to_string());
        } else {
            ids = vec![ids.first().unwrap().clone(), prefix.to_string()];
        }
        Self(ids)
    }
    pub fn from_iter<I: IntoIterator<Item = String>>(iter: I) -> Result<Self, ValidateError> {
        let mut iter = iter.into_iter().collect::<Vec<_>>().into_iter().rev();
        if let Some(ty) = iter.next() {
            let mut out = vec![ty];
            out.extend(iter);
            Ok(Self(out))
        } else {
            Err(ValidateError::InvalidIdentifier("EMPTY".to_string()))
        }
    }
}

impl TryFrom<sqlparser::ast::ObjectName> for ContextKey {
    type Error = ValidateError;
    fn try_from(on: sqlparser::ast::ObjectName) -> ValidateResult<Self> {
        let mut it = on.0.into_iter().rev();
        let name = it
            .next()
            .ok_or(ValidateError::InvalidIdentifier("empty".to_string()))?;
        let mut out = ContextKey::with_name(&name);
        for p in it {
            out = out.and_prefix(&p);
        }
        Ok(out)
    }
}

impl std::str::FromStr for ContextKey {
    type Err = ValidateError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let buf: Vec<String> = s.split(".").map(|s| s.to_string()).collect();
        let mut it = buf.into_iter().rev();
        let name = it
            .next()
            .ok_or(ValidateError::InvalidIdentifier(s.to_string()))?;
        let mut out = Self::with_name(&name);
        while let Some(prefix) = it.next() {
            out = out.and_prefix(&prefix);
        }
        Ok(out)
    }
}

impl<'a> std::iter::Extend<&'a str> for ContextKey {
    fn extend<T: IntoIterator<Item = &'a str>>(&mut self, iter: T) {
        self.0.extend(iter.into_iter().map(|s| s.to_string()));
    }
}

impl std::fmt::Display for ContextKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut ids = self.0.iter().rev();
        write!(f, "`{}`", ids.next().unwrap())?;
        while let Some(val) = ids.next() {
            write!(f, ".`{}`", val)?;
        }
        Ok(())
    }
}

/// A wrapper to `M` in a given context, and allow looking them up by using
/// expressions of the form `[root.][major.]minor`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Context<M> {
    ctx: Vec<(ContextKey, M)>,
}

#[async_trait]
pub trait Contextish {
    type M;
    async fn get(&self, key: &ContextKey) -> Result<&Self::M, ContextError>;
}

impl<M> Default for Context<M> {
    fn default() -> Self {
        Self { ctx: Vec::new() }
    }
}

pub trait ToContext {
    type M;
    fn to_context(&self) -> Context<Self::M>;
}

pub trait TryToContext {
    type M;
    fn try_to_context(&self) -> ValidateResult<Context<Self::M>>;
}

impl<M> ToContext for Context<M>
where
    M: Clone,
{
    type M = M;
    fn to_context(&self) -> Self {
        self.clone()
    }
}

impl<M> Context<M>
where
    M: ToContext,
{
    fn flatten(&self) -> Context<<M as ToContext>::M> {
        let ctx: Vec<_> = self
            .ctx
            .iter()
            .flat_map(|(k, m)| {
                m.to_context().ctx.into_iter().map(move |(mut i_k, i_m)| {
                    i_k.extend(k.iter());
                    (i_k, i_m)
                })
            })
            .collect();
        Context { ctx }
    }
}

impl<M> std::iter::IntoIterator for Context<M> {
    type Item = (ContextKey, M);
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.ctx.into_iter()
    }
}

impl<M> std::iter::Extend<(ContextKey, M)> for Context<M> {
    fn extend<T: IntoIterator<Item = (ContextKey, M)>>(&mut self, iter: T) {
        self.ctx.extend(iter)
    }
}

impl<'a, M: 'a> std::iter::Extend<&'a (ContextKey, M)> for Context<M>
where
    M: Clone,
{
    fn extend<T: IntoIterator<Item = &'a (ContextKey, M)>>(&mut self, iter: T) {
        self.ctx.extend(iter.into_iter().cloned())
    }
}

#[derive(Debug)]
pub enum ContextError {
    NotFound(ContextKey),
    Ambiguous(ContextKey),
}

impl ContextError {
    pub fn into_column_error(self) -> ValidateError {
        match self {
            ContextError::NotFound(key) => ValidateError::ColumnNotFound(key.to_string()),
            ContextError::Ambiguous(key) => ValidateError::AmbiguousColumnName(key.to_string()),
        }
    }

    pub fn into_table_error(self) -> ValidateError {
        match self {
            ContextError::NotFound(key) => ValidateError::TableNotFound(key),
            ContextError::Ambiguous(key) => ValidateError::AmbiguousTableName(key),
        }
    }
}

impl std::fmt::Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(ck) => write!(f, "not found in context: {}", ck),
            Self::Ambiguous(ck) => write!(f, "ambiguous in context: {}", ck),
        }
    }
}

impl std::error::Error for ContextError {}

impl Context<ExprMeta> {
    fn get_column(&self, key: &ContextKey) -> Result<&ExprMeta, ValidateError> {
        self.get(key).map_err(|e| e.into_column_error())
    }
}

impl Context<TableMeta> {
    pub(crate) fn get_table(&self, key: &ContextKey) -> Result<&TableMeta, ValidateError> {
        self.get(key).map_err(|e| e.into_table_error())
    }
}

impl<M> Context<M> {
    pub fn new() -> Self {
        Self { ctx: Vec::new() }
    }

    pub fn insert(&mut self, key: ContextKey, m: M) {
        self.ctx.push((key, m))
    }

    pub fn get(&self, key: &ContextKey) -> Result<&M, ContextError> {
        let mut matches = self
            .ctx
            .iter()
            .filter(|(k, _)| key.matches(k))
            .collect::<Vec<_>>();
        match matches.len() {
            0 => Err(ContextError::NotFound(key.clone())),
            1 => Ok(&matches.first().unwrap().1),
            _ => Err(ContextError::Ambiguous(key.clone())),
        }
    }

    pub fn get_inv(&self, key: &ContextKey) -> Result<&M, ContextError> {
        let mut matches = self
            .ctx
            .iter()
            .filter(|(k, _)| key.inv_matches(k))
            .collect::<Vec<_>>();
        match matches.len() {
            0 => Err(ContextError::NotFound(key.clone())),
            1 => Ok(&matches.first().unwrap().1),
            _ => Err(ContextError::Ambiguous(key.clone())),
        }
    }

    pub fn get_mut(&mut self, key: &ContextKey) -> Result<&mut M, ContextError> {
        let mut matches = self
            .ctx
            .iter_mut()
            .filter(|(k, _)| key.matches(k))
            .collect::<Vec<_>>();
        match matches.len() {
            0 => Err(ContextError::NotFound(key.clone())),
            1 => Ok(&mut matches.pop().unwrap().1),
            _ => Err(ContextError::Ambiguous(key.clone())),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &(ContextKey, M)> {
        self.ctx.iter()
    }

    pub fn iter_values(&self) -> impl Iterator<Item = &M> {
        self.ctx.iter().map(|(_, v)| v)
    }

    pub fn keys(&self) -> impl Iterator<Item = &ContextKey> {
        self.ctx.iter().map(|(k, v)| k)
    }

    pub fn sha256(&self) -> String {
        base64::encode(&[0u8; 32]) // TODO
    }
}

impl<V> std::iter::FromIterator<(ContextKey, V)> for Context<V> {
    fn from_iter<T: IntoIterator<Item = (ContextKey, V)>>(iter: T) -> Self {
        let ctx = Vec::from_iter(iter);
        Self { ctx }
    }
}

#[cfg(test)]
pub mod tests {}
