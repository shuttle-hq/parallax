use sqlparser::ast;

use crate::common::*;

use super::{
    expr::*, CompositionError, Context, ContextKey, ExprAnsatz, RelAnsatz, RelTryComplete,
    ToAnsatz, ToContext, TryToContext, ValidateError, ValidateResult,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum JoinConstraint {
    On(ExprT),
    Using(Vec<ContextKey>),
    Natural,
}

impl TryInto<ast::JoinConstraint> for JoinConstraint {
    type Error = CompositionError;
    fn try_into(self) -> Result<ast::JoinConstraint, Self::Error> {
        let out = match self {
            Self::On(expr) => ast::JoinConstraint::On(expr.to_ansatz()?.into()),
            Self::Using(key) => {
                let key = key.into_iter().map(|ck| ck.name().to_string()).collect();
                ast::JoinConstraint::Using(key)
            }
            Self::Natural => ast::JoinConstraint::Natural,
        };
        Ok(out)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
    RightOuter(JoinConstraint),
    FullOuter(JoinConstraint),
    CrossJoin,
}

impl TryInto<ast::JoinOperator> for JoinOperator {
    type Error = CompositionError;
    fn try_into(self) -> Result<ast::JoinOperator, Self::Error> {
        let out = match self {
            JoinOperator::Inner(cst) => ast::JoinOperator::Inner(cst.try_into()?),
            JoinOperator::LeftOuter(cst) => ast::JoinOperator::LeftOuter(cst.try_into()?),
            JoinOperator::RightOuter(cst) => ast::JoinOperator::RightOuter(cst.try_into()?),
            JoinOperator::FullOuter(cst) => ast::JoinOperator::FullOuter(cst.try_into()?),
            JoinOperator::CrossJoin => ast::JoinOperator::CrossJoin,
        };
        Ok(out)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Order {
    Asc,
    Desc,
}

copy_ast_enum!(
    #[derive(Serialize, Deserialize, Debug, Clone,)]
    pub enum ast::SetOperator as SetOperator {
        Union,
        Except,
        Intersect,
    }
);

entish! {
    #[derive(Map, MapOwned, From, TryInto, IntoResult)]
    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[entish(variants_as_structs)]
    pub enum GenericRel<Expr> {
        Aggregation {
            pub attributes: Vec<Expr>,
            pub group_by: Vec<Expr>,
            pub from: Self
        },
        Projection {
            pub attributes: Vec<Expr>,
            pub from: Self,
        },
        Selection {
            pub from: Self,
            pub where_: Expr
        },
        Join {
            pub left: Self,
            pub right: Self,
            pub operator: JoinOperator
        },
        Set {
            pub operator: SetOperator,
            pub left: Self,
            pub right: Self
        },
        Offset {
            pub offset: Expr,
            pub from: Self
        },
        Limit {
            pub number_rows: Expr,
            pub from: Self
        },
        OrderBy {
            pub order: Vec<Order>,
            pub by: Vec<Expr>,
            pub from: Self
        },
        Distinct {
            pub from: Self
        },
        WithAlias {
            pub from: Self,
            pub alias: String
        },
        Table(pub ContextKey)
    }
}

to_ansatz! {
    match<Expr,> GenericRel<RelAnsatz> {
        OrderBy<Expr,> { order, by, from } => {
            let mut out: ast::Query = from.into();
            out.order_by = by
                .into_iter()
                .zip(order.into_iter())
                .map(|(by, order)| {
                    let asc = match order {
                        Order::Asc => true,
                        Order::Desc => false
                    };
                    let obe = ast::OrderByExpr {
                        asc: Some(asc),
                        expr: by.to_ansatz()?.into()
                    };
                    Ok(obe)
                })
                .collect::<Result<Vec<_>, _>>()?;
            out
        },
        Distinct<> { from } => {
            let mut out: ast::Select = from.into();
            out.distinct = true;
            out
        },
        Limit<Expr,> { number_rows, from } => {
            let mut out: ast::Query = from.into();
            out.limit = Some(number_rows.to_ansatz()?.into());
            out
        },
        Offset<Expr,> { offset, from } => {
            let mut out: ast::Query = from.into();
            out.offset = Some(offset.to_ansatz()?.into());
            out
        },
        Set<> { operator, left, right } => {
            ast::SetExpr::SetOperation {
                op: operator.into(),
                all: false,
                left: Box::new(left.into()),
                right: Box::new(right.into())
            }
        },
        Join<> { left, right, operator } => {
            let mut out: ast::TableWithJoins = left.into();
            let join = ast::Join {
                relation: right.into(),
                join_operator: operator.try_into()?
            };
            out.joins.push(join);
            out
        },
        Selection<Expr,> { from, where_ } => {
            let mut out: ast::Select = from.into();
            out.selection = Some(where_.to_ansatz()?.into());
            out
        },
        Aggregation<Expr,> { attributes, group_by, from } => {
            let mut out: ast::Select = from.into();
            out.projection = attributes
                .into_iter()
                .map(|attr| Ok(attr.to_ansatz()?.into()))
                .collect::<Result<Vec<_>, _>>()?;
            out.group_by = group_by
                .into_iter()
                .map(|attr| Ok(attr.to_ansatz()?.into()))
                .collect::<Result<Vec<_>, _>>()?;
            out
        },
        Projection<Expr,> { attributes, from } => {
            let mut out: ast::Select = from.into();
            out.projection = attributes
                .into_iter()
                .map(|attr| Ok(attr.to_ansatz()?.into()))
                .collect::<Result<Vec<_>, _>>()?;
            out
        },
        WithAlias<> { from, alias } => {
            from.with_alias(&alias)
        },
        #[leaf] Table<>(key) => {
            let mut ident = key.0;
            ident.reverse();
            ast::TableFactor::Table {
                name: ast::ObjectName(ident),
                alias: None,
                args: vec![],
                with_hints: vec![]
            }
        },
    }
}

pub type Rel<T> = GenericRel<ExprT, T>;

/// Relation Tree
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RelT<B = TableMeta> {
    pub(crate) root: Rel<Arc<RelT<B>>>,
    pub(crate) board: ValidateResult<B>,
}

impl<B> RelT<B> {
    pub fn is_leaf(&self) -> bool {
        match &self.root {
            Rel::Table(..) => true,
            _ => false,
        }
    }
}

impl<B> RelT<B>
where
    B: RelTryComplete,
{
    pub fn from<T: Into<Rel<Self>>>(t: T) -> Self {
        let rel: Rel<Self> = t.into();
        let root = rel.map_owned(&mut |c| Arc::new(c));
        Self::from_wrapped(root)
    }

    pub fn from_wrapped<T: Into<Rel<Arc<Self>>>>(t: T) -> Self {
        let root: Rel<Arc<Self>> = t.into();
        let board = root
            .map(&mut |c| c.board.as_ref())
            .into_result()
            .map_err(|e| e.clone())
            .and_then(|n| B::try_complete(n));
        Self { root, board }
    }
}

impl TryToContext for RelT {
    type M = ExprMeta;
    fn try_to_context(&self) -> ValidateResult<Context<Self::M>> {
        self.board
            .as_ref()
            .map(|table_meta| table_meta.to_context())
            .map_err(|e| e.clone())
    }
}

impl<T> RelTree for T where T: GenericRelTree<ExprT> {}

pub trait RelTree: GenericRelTree<ExprT> {}

impl GenericRelTree<ExprT> for RelT {
    fn as_ref(&self) -> Rel<&Self> {
        self.root.map(&mut |n| n.as_ref())
    }
    fn into_inner(self) -> Rel<Self> {
        self.root.map_owned(&mut |n| (*n).clone())
    }
}

impl ToAnsatz for RelT {
    type Ansatz = RelAnsatz;
    fn to_ansatz(self) -> Result<Self::Ansatz, CompositionError> {
        self.try_fold(&mut |t| t.to_ansatz())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct TableMeta {
    pub columns: Context<ExprMeta>,
    pub loc: Option<BlockType>,
    pub source: Option<ContextKey>,
    pub audience: HashSet<BlockType>,
}

impl TableMeta {
    pub fn from(ctx: Context<ExprMeta>) -> ValidateResult<Self> {
        Ok(Self {
            columns: ctx,
            ..Default::default()
        })
    }
}

impl ToContext for TableMeta {
    type M = ExprMeta;
    fn to_context(&self) -> Context<Self::M> {
        self.columns.clone()
    }
}

impl RelTryComplete for TableMeta {
    fn try_complete(node: Rel<&Self>) -> ValidateResult<Self> {
        let columns: Context<ExprMeta> = match &node {
            Rel::WithAlias(WithAlias { from, alias }) => {
                let ctx = from
                    .to_context()
                    .into_iter()
                    .map(|(k, v)| (k.with_prefix(&alias), v.clone()))
                    .collect();
                Ok(ctx)
            }
            Rel::Table(Table(_)) => Err(ValidateError::Internal(
                "tried to complete from leaf".to_string(),
            )),
            Rel::Projection(Projection { attributes, from })
            | Rel::Aggregation(Aggregation {
                attributes, from, ..
            }) => {
                let ctx: Context<ExprMeta> = attributes
                    .iter()
                    .enumerate()
                    .map(|(i, expr)| {
                        let key = if let Expr::As(As { alias, .. }) = &expr.root {
                            ContextKey::with_name(alias)
                        } else {
                            ContextKey::with_name(&format!("f{}_", i))
                        };
                        Ok((key, expr.board.clone()?))
                    })
                    .collect::<ValidateResult<_>>()?;
                Ok(ctx)
            }
            Rel::Offset(Offset { from, .. })
            | Rel::Limit(Limit { from, .. })
            | Rel::OrderBy(OrderBy { from, .. })
            | Rel::Distinct(Distinct { from, .. })
            | Rel::Selection(Selection { from, .. }) => Ok(from.columns.clone()),
            Rel::Join(Join { left, right, .. }) => {
                let mut ctx = left.to_context();
                ctx.extend(right.to_context().into_iter());
                Ok(ctx)
            }
            Rel::Set(Set { left, right, .. }) => {
                let right = right.to_context();
                let ctx: Context<ExprMeta> = left
                    .to_context()
                    .into_iter()
                    .map(|(key, meta)| {
                        let right_m = right.get_column(&key)?;

                        if meta == *right_m {
                            Ok((ContextKey::with_name(key.name()), meta))
                        } else {
                            Err(ValidateError::SchemaMismatch(key.to_string()))
                        }
                    })
                    .collect::<ValidateResult<_>>()?;
                Ok(ctx)
            }
        }?;

        let mut locs = HashSet::new();
        node.map(&mut |table_meta| {
            if let Some(loc) = table_meta.loc.as_ref() {
                locs.insert(loc);
            }
        });

        let source = None;

        let loc = if locs.len() == 1 {
            Some((*locs.iter().next().unwrap()).clone())
        } else {
            None
        };

        let mut audiences = Vec::new();
        node.map(&mut |child| {
            audiences.push(&child.audience);
        });

        let mut audience = audiences
            .pop()
            .map(|aud| aud.clone())
            .unwrap_or(HashSet::new());
        for aud in audiences.into_iter() {
            audience = audience.intersection(aud).cloned().collect();
        }

        Ok(Self {
            columns,
            loc,
            source,
            audience,
        })
    }
}
