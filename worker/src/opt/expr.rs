use sqlparser::ast;

use crate::common::*;

use super::{
    AudienceBoard, CompositionError, Context, ContextKey, DataType, Domain, ExprAnsatz,
    ExprTryComplete, Mode, ToAnsatz, ToContext, ValidateError, ValidateResult,
};

copy_ast_enum!(
    #[derive(Serialize, Deserialize, Debug, Clone,)]
    pub enum ast::BinaryOperator as BinaryOperator {
        Plus,
        Minus,
        Multiply,
        Divide,
        Modulus,
        Gt,
        Lt,
        GtEq,
        LtEq,
        Eq,
        NotEq,
        And,
        Or,
        Like,
        NotLike,
    }
);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum LiteralValue {
    Long(i64),
    Boolean(bool),
    Double(f64),
    StringLiteral(String),
    Null,
}

impl<'a> TryFrom<&'a ast::Value> for LiteralValue {
    type Error = ValidateError;
    fn try_from(value: &'a ast::Value) -> ValidateResult<Self> {
        match value {
            ast::Value::Number(nstr) => i64::from_str(nstr)
                .map(|num| LiteralValue::Long(num))
                .or_else(|_| f64::from_str(nstr).map(|d| LiteralValue::Double(d)))
                .map_err(|_| ValidateError::InvalidLiteral(format!("number literal {}", nstr))),
            ast::Value::Boolean(b) => Ok(LiteralValue::Boolean(*b)),
            ast::Value::Null => Ok(LiteralValue::Null),
            _ => Err(ValidateError::Wip(format!("literal {}", value))),
        }
    }
}

copy_ast_enum!(
    #[derive(Serialize, Deserialize, Debug, Clone,)]
    pub enum ast::UnaryOperator as UnaryOperator {
        Plus,
        Minus,
        Not,
    }
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum FunctionName {
    Sum,
    Avg,
    Count,
    StdDev,
    Max,
    Min,
}

impl<'a> TryFrom<&'a ast::ObjectName> for FunctionName {
    type Error = ValidateError;
    fn try_from(value: &'a ast::ObjectName) -> ValidateResult<Self> {
        if value.0.len() == 1 {
            let name = value.0.get(0).unwrap();
            Self::from_str(name)
        } else {
            let value_str = value.0.as_slice().join(".");
            Err(ValidateError::InvalidFunctionName(value_str))
        }
    }
}

impl std::fmt::Display for FunctionName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Sum => "SUM",
                Self::Avg => "AVG",
                Self::Count => "COUNT",
                Self::StdDev => "STDDEV",
                Self::Max => "MAX",
                Self::Min => "MIN",
            }
        )
    }
}

impl FromStr for FunctionName {
    type Err = ValidateError;
    fn from_str(s: &str) -> ValidateResult<Self> {
        match s {
            "SUM" => Ok(FunctionName::Sum),
            "AVG" => Ok(FunctionName::Avg),
            "COUNT" => Ok(FunctionName::Count),
            "STDDEV" => Ok(FunctionName::StdDev),
            "MAX" => Ok(FunctionName::Max),
            "MIN" => Ok(FunctionName::Min),
            _ => Err(ValidateError::InvalidFunctionName(s.to_string())),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum HashAlgorithm {
    SHA256, // FIXME
}

impl std::default::Default for HashAlgorithm {
    fn default() -> Self {
        Self::SHA256 // FIXME
    }
}

entish! {
    #[derive(Map, MapOwned, From, TryInto, IntoResult)]
    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[entish(variants_as_structs)]
    pub enum Expr {
        IsNull(pub Self),
        IsNotNull(pub Self),
        InList {
            pub expr: Self,
            pub list: Vec<Self>,
            pub negated: bool
        },
        // suppressed InSubquery
        Between {
            pub expr: Self,
            pub negated: bool,
            pub low: Self,
            pub high: Self
        },
        BinaryOp {
            pub left: Self,
            pub op: BinaryOperator,
            pub right: Self
        },
        UnaryOp {
            pub op: UnaryOperator,
            pub expr: Self
        },
        Literal(pub LiteralValue),
        Function {
            pub name: FunctionName,
            pub args: Vec<Self>,
            pub distinct: bool
        },
        Case {
            pub operand: Option<Self>,
            pub conditions: Vec<Self>,
            pub results: Vec<Self>,
            pub else_results: Option<Self>
        },
        As {
            pub expr: Self,
            pub alias: String
        },
        Column(pub ContextKey),
        Hash {
            pub algo: HashAlgorithm,
            pub expr: Self,
            pub salt: Vec<u8>
        },
        Replace {
            pub expr: Self,
            pub with: Self
        },
    }
}

to_ansatz! {
    match<> Expr<ExprAnsatz> {
        Case<> { operand, conditions, results, else_results } => {
            ast::Expr::Case {
                operand: operand.map(|op| Box::new(op.into())),
                conditions: conditions.into_iter().map(|a| a.into()).collect(),
                results: results.into_iter().map(|a| a.into()).collect(),
                else_result: else_results.map(|er| Box::new(er.into()))
            }
        },
        #[leaf] Literal<>(lit) => {
            let value = match lit {
                LiteralValue::Long(v) => ast::Value::Number(v.to_string()),
                LiteralValue::Boolean(b) => ast::Value::Boolean(b),
                LiteralValue::Double(v) => ast::Value::Number(v.to_string()),
                LiteralValue::StringLiteral(lit) => ast::Value::SingleQuotedString(lit),
                LiteralValue::Null => ast::Value::Null
            };
            ast::Expr::Value(value)
        },
        UnaryOp<> { op, expr } => {
            ast::Expr::UnaryOp {
                op: op.into(),
                expr: Box::new(expr.into())
            }
        },
        BinaryOp<> { left, op, right } => {
            ast::Expr::BinaryOp {
                left: Box::new(left.into()),
                op: op.into(),
                right: Box::new(right.into())
            }
        },
        Between<> { expr, negated, low, high } => {
            ast::Expr::Between {
                expr: Box::new(expr.into()),
                negated,
                low: Box::new(low.into()),
                high: Box::new(high.into())
            }
        },
        InList<> { expr, list, negated } => {
            ast::Expr::InList {
                expr: Box::new(expr.into()),
                list: list
                    .into_iter()
                    .map(|expr| expr.into())
                    .collect(),
                negated
            }
        },
        IsNotNull<>(expr) => {
            ast::Expr::IsNotNull(Box::new(expr.into()))
        },
        IsNull<>(expr) => {
            ast::Expr::IsNull(Box::new(expr.into()))
        },
        Function<> { name, args, distinct } => {
            ast::Expr::Function(
                ast::Function {
                    name: ast::ObjectName(vec![name.to_string()]),
                    args: args
                        .into_iter()
                        .map(|arg| arg.into())
                        .collect(),
                    over: None,
                    distinct
                }
            )
        },
        As<> { expr, alias } => {
            expr.with_alias(&alias)
        },
        #[leaf] Column<>(mut key) => {
            let mut ident = key.0;
            ident.reverse();
            ast::Expr::CompoundIdentifier(ident)
        },
        Replace<> { expr, with } => {
            with
        },
    }
}

impl ExprTree for ExprT {
    fn as_ref(&self) -> Expr<&Self> {
        self.root.map(&mut |n| n.as_ref())
    }
    fn into_inner(self) -> Expr<Self> {
        self.root.map_owned(&mut |n| (*n).clone())
    }
}

impl ToAnsatz for ExprT {
    type Ansatz = ExprAnsatz;
    fn to_ansatz(self) -> Result<Self::Ansatz, CompositionError> {
        self.try_fold(&mut |t| t.to_ansatz())
    }
}

derive_try_complete_expr! {
    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq,)]
    pub struct ExprMeta {
        ty: DataType,
        domain: Domain,
        audience: HashSet<BlockType>,
        mode: Mode,
    }
}

impl Default for ExprMeta {
    fn default() -> Self {
        Self {
            ty: DataType::default(),
            domain: Domain::default(),
            audience: HashSet::new(),
            mode: Mode::default(),
        }
    }
}

impl<I> Expr<I>
where
    I: IntoIterator,
{
    pub fn coalesce(self) -> impl Iterator<Item = Expr<I::Item>> {
        std::iter::once(unimplemented!())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExprT<B = ExprMeta> {
    pub(crate) root: Expr<Arc<ExprT<B>>>,
    pub(crate) board: ValidateResult<B>,
}

#[async_trait]
pub trait RebaseExpr<Meta>: Send + Sync {
    type NewMeta: ExprTryComplete + Send + Sync;
    async fn maybe_at(&self, at: &ExprT<Meta>) -> Option<ExprT<Self::NewMeta>>;
}

#[async_trait]
impl RebaseExpr<ExprMeta> for Context<ExprMeta> {
    type NewMeta = ExprMeta;
    async fn maybe_at(&self, at: &ExprT<ExprMeta>) -> Option<ExprT<Self::NewMeta>> {
        match &at.root {
            Expr::Column(Column(context_key)) => Some(ExprT {
                root: Expr::Column(Column(context_key.clone())),
                board: self
                    .get(context_key)
                    .map(|v| v.clone())
                    .map_err(|e| e.into_column_error()),
            }),
            _ => None,
        }
    }
}

impl<B> ExprT<B> {
    pub fn is_leaf(&self) -> bool {
        match &self.root {
            Expr::Literal(..) | Expr::Column(..) => true,
            _ => false,
        }
    }
}

// This stuff is boilerplate and should be in Entish
impl<C> Expr<C> {
    pub async fn map_async<'a, F, Fut, CC>(&'a self, f: F) -> Expr<CC>
    where
        F: Fn(&'a C) -> Fut,
        Fut: Future<Output = CC> + Send + 'a,
    {
        map_variants!(
            self as Expr {
                InList => {
                    expr: { f(expr).await },
                    list: { join_all(list.iter().map(|elt| f(elt))).await },
                    negated: { *negated },
                },
                Between => {
                    expr: { f(expr).await },
                    negated: { *negated },
                    low: { f(low).await },
                    high: { f(high).await },
                },
                BinaryOp => {
                    left: { f(left).await },
                    op: { op.clone() },
                    right: { f(right).await },
                },
                UnaryOp => {
                    op: { op.clone() },
                    expr: { f(expr).await },
                },
                Function => {
                    name: { name.clone() },
                    args: { join_all(args.iter().map(|elt| f(elt))).await },
                    distinct: { *distinct },
                },
                Case => {
                    operand: {
                        if let Some(operand) = operand.as_ref() {
                            Some(f(operand).await)
                        } else {
                            None
                        }
                    },
                    conditions: { join_all(conditions.iter().map(|elt| f(elt))).await },
                    results: { join_all(results.iter().map(|elt| f(elt))).await },
                    else_results: {
                        if let Some(else_results) = else_results.as_ref() {
                            Some(f(else_results).await)
                        } else {
                            None
                        }
                    },
                },
                As => {
                    expr: { f(expr).await },
                    alias: { alias.clone() },
                },
                Hash => {
                    algo: { algo.clone() },
                    expr: { f(expr).await },
                    salt: { salt.clone() },
                },
                Replace => {
                    expr: { f(expr).await },
                    with: { f(with).await },
                },
                #[unnamed] Column => { context_key: { context_key.clone() }, },
                #[unnamed] Literal => { lit: { lit.clone() }, },
                #[unnamed] IsNull => { from: { f(from).await }, },
                #[unnamed] IsNotNull => { from: { f(from).await }, },
            }
        )
    }
}

impl<B> ExprT<B>
where
    B: Send + Sync,
{
    pub fn rebase<'a, R>(&'a self, r: &'a R) -> impl Future<Output = ExprT<R::NewMeta>> + Send + 'a
    where
        R: RebaseExpr<B> + Send + Sync,
    {
        async move {
            if let Some(new_base) = r.maybe_at(self).await {
                new_base
            } else {
                let rebased_root = self
                    .root
                    .map_async(async move |child| child.rebase(r).await)
                    .await;
                ExprT::from(rebased_root)
            }
        }
        .boxed()
    }
}

impl<B> ExprT<B>
where
    B: ExprTryComplete,
{
    pub fn from<T: Into<Expr<Self>>>(t: T) -> Self {
        let expr: Expr<Self> = t.into();
        let root = expr.map_owned(&mut |c| Arc::new(c));
        let board = root
            .map(&mut |c| c.board.as_ref())
            .into_result()
            .map_err(|e| e.clone())
            .and_then(|n| B::try_complete(n));
        Self { root, board }
    }
}
