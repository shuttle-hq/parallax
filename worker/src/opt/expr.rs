use sqlparser::ast;

use crate::common::*;

use super::{
    AudienceBoard, CompositionError, Context, ContextKey, DataType, Domain, ExprAnsatz, ExprRepr,
    Mode, Named, ToAnsatz, ToContext, ValidateError, ValidateResult,
};

macro_rules! sql_parser_expr {
    ($lit:literal $(, $key:ident = $val:tt)*) => {
        {
            let sql = format!($lit $(, $key = $val)*);
            let mut tokenizer = sqlparser::tokenizer::Tokenizer::new(
                &sqlparser::dialect::GenericDialect {},
                &sql
            );
            let tokens = tokenizer.tokenize().unwrap();
            let mut parser = sqlparser::parser::Parser::new(tokens);
            parser.parse_expr().unwrap()
        }
    };
}

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

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum Distribution {
    Laplace { mean: f64, variance: f64 }, // FIXME
}

entish! {
    #[derive(Map, MapOwned, From, TryInto, IntoResult, IntoOption)]
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
        Noisy {
            pub expr: Self,
            pub distribution: Distribution
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
        Noisy<> { expr, distribution } => {
            match distribution {
                Distribution::Laplace { mean, variance } => {
                    // TODO: This obviously needs a bit of codegen help...
                    ast::Expr::BinaryOp {
                        left: Box::new(expr.into()),
                        op: ast::BinaryOperator::Plus,
                        right: Box::new(sql_parser_expr!(
                            "{mean} - {variance} * (SIGN(RAND() - 0.5) * LN(1 - 2*ABS(RAND() - 0.5)))",
                            mean = mean,
                            variance = variance
                        ))
                    }
                },
            }
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

#[derive(Default, Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ExprAlias(Option<String>);

impl ExprAlias {
    pub fn to_str(&self) -> Option<&str> {
        self.0.as_ref().map(|s| s.as_str())
    }
}

impl ExprRepr for ExprAlias {
    fn dot(node: Expr<&Self>) -> ValidateResult<Self> {
        match node {
            Expr::As(As { alias, .. }) => Ok(Self(Some(alias.clone()))),
            _ => Ok(Self(None)),
        }
    }
}

derive_expr_repr! {
    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq,)]
    pub struct ExprMeta {
        pub ty: DataType,
        pub audience: HashSet<BlockType>,
        pub mode: Mode,
        pub alias: ExprAlias,
    }
}

impl Named for ExprMeta {
    fn name(&self) -> Option<&str> {
        self.alias.0.as_ref().map(|s| s.as_str())
    }
}

impl Default for ExprMeta {
    fn default() -> Self {
        Self {
            ty: DataType::default(),
            audience: HashSet::new(),
            mode: Mode::default(),
            alias: ExprAlias::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExprT<B = ExprMeta> {
    pub(crate) root: Expr<Arc<ExprT<B>>>,
    pub(crate) board: ValidateResult<B>,
}

use futures::future::ready;

impl<B: Clone> ExprT<B> {
    pub fn from_context(key: ContextKey, ctx: &Context<B>) -> Self {
        Self {
            root: Expr::Column(Column(key.clone())),
            board: ctx
                .get(&key)
                .map(|b| b.clone())
                .map_err(|e| e.into_column_error()),
        }
    }
}

/// A rule for rebasing an expression tree with existing label `Meta`.
pub trait RebaseExpr<From: ExprRepr>: Send + Sync {
    type To: ExprRepr;
    fn rebase_at(&self, at: &ExprT<From>) -> Option<ExprT<Self::To>>;

    fn rebase(&self, root: &ExprT<From>) -> ExprT<Self::To> {
        if let Some(new_base) = self.rebase_at(root) {
            new_base
        } else {
            let rebased_root = root.root.map(&mut |child| self.rebase(child));
            ExprT::from(rebased_root)
        }
    }
}

impl<E, M> RebaseExpr<E> for Context<M>
where
    E: ExprRepr,
    M: ExprRepr + Send + Sync + Clone,
{
    type To = M;
    fn rebase_at(&self, at: &ExprT<E>) -> Option<ExprT<Self::To>> {
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
                Noisy => {
                    expr: { f(expr).await },
                    distribution: { distribution.clone() },
                },
                #[unnamed] Column => { context_key: { context_key.clone() }, },
                #[unnamed] Literal => { lit: { lit.clone() }, },
                #[unnamed] IsNull => { from: { f(from).await }, },
                #[unnamed] IsNotNull => { from: { f(from).await }, },
            }
        )
    }
}

impl<E: ExprRepr> ExprT<E> {
    pub fn from<T: Into<Expr<Self>>>(t: T) -> Self {
        let expr: Expr<Self> = t.into();
        let root = expr.map_owned(&mut |c| Arc::new(c));
        let board = root
            .map(&mut |c| c.board.as_ref())
            .into_result()
            .map_err(|e| e.clone())
            .and_then(|n| E::dot(n));
        Self { root, board }
    }
}
