use sqlparser::ast;

use crate::common::*;

use super::{
    expr::*, CompositionError, Context, ContextKey, ExprAnsatz, ExprTryComplete, RebaseExpr,
    RelAnsatz, RelTryComplete, ToAnsatz, ToContext, TryToContext, ValidateError, ValidateResult,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum JoinConstraint<E = ExprT> {
    On(E),
    Using(Vec<ContextKey>),
    Natural,
}

impl<E> JoinConstraint<E> {
    pub async fn map_expressions_async<'a, O, F, Fut>(&'a self, f: F) -> JoinConstraint<O>
    where
        F: Fn(&'a E) -> Fut,
        Fut: Future<Output = O> + 'a,
    {
        match self {
            JoinConstraint::On(e) => JoinConstraint::On(f(e).await),
            JoinConstraint::Using(what) => JoinConstraint::Using(what.clone()),
            JoinConstraint::Natural => JoinConstraint::Natural,
        }
    }
    pub fn map_expressions<O, F: Fn(&E) -> O>(&self, f: F) -> JoinConstraint<O> {
        match self {
            JoinConstraint::On(e) => JoinConstraint::On(f(e)),
            JoinConstraint::Using(what) => JoinConstraint::Using(what.clone()),
            JoinConstraint::Natural => JoinConstraint::Natural,
        }
    }
}

impl<E> TryInto<ast::JoinConstraint> for JoinConstraint<E>
where
    E: ToAnsatz<Ansatz = ExprAnsatz>,
{
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
pub enum JoinOperator<E = ExprT> {
    Inner(JoinConstraint<E>),
    LeftOuter(JoinConstraint<E>),
    RightOuter(JoinConstraint<E>),
    FullOuter(JoinConstraint<E>),
    CrossJoin,
}

impl<E> TryInto<ast::JoinOperator> for JoinOperator<E>
where
    E: ToAnsatz<Ansatz = ExprAnsatz>,
{
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

impl<E> JoinOperator<E> {
    pub async fn map_expressions_async<'a, O, F, Fut>(&'a self, f: F) -> JoinOperator<O>
    where
        F: Fn(&'a E) -> Fut,
        Fut: Future<Output = O> + Send + 'a,
    {
        match self {
            JoinOperator::Inner(inner) => JoinOperator::Inner(inner.map_expressions_async(f).await),
            JoinOperator::LeftOuter(inner) => {
                JoinOperator::LeftOuter(inner.map_expressions_async(f).await)
            }
            JoinOperator::RightOuter(inner) => {
                JoinOperator::RightOuter(inner.map_expressions_async(f).await)
            }
            JoinOperator::FullOuter(inner) => {
                JoinOperator::FullOuter(inner.map_expressions_async(f).await)
            }
            JoinOperator::CrossJoin => JoinOperator::CrossJoin,
        }
    }
    fn map_expressions<O, F: Fn(&E) -> O>(&self, f: F) -> JoinOperator<O> {
        match self {
            JoinOperator::Inner(inner) => JoinOperator::Inner(inner.map_expressions(f)),
            JoinOperator::LeftOuter(inner) => JoinOperator::LeftOuter(inner.map_expressions(f)),
            JoinOperator::RightOuter(inner) => JoinOperator::RightOuter(inner.map_expressions(f)),
            JoinOperator::FullOuter(inner) => JoinOperator::FullOuter(inner.map_expressions(f)),
            JoinOperator::CrossJoin => JoinOperator::CrossJoin,
        }
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
            pub operator: JoinOperator<Expr>
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
        Join<Expr,> { left, right, operator } => {
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
pub struct RelT<B = TableMeta, E = ExprT> {
    pub(crate) root: GenericRel<E, Arc<RelT<B, E>>>,
    pub(crate) board: ValidateResult<B>,
}

// This stuff is boilerplate and should be in Entish
impl<E, C> GenericRel<E, C>
where
    E: Clone,
    C: Clone,
{
    pub async fn map_async<'a, F, Fut, CC>(&'a self, f: F) -> GenericRel<E, CC>
    where
        F: Fn(&'a C) -> Fut,
        Fut: Future<Output = CC> + Send + 'a,
    {
        map_variants!(
            self as GenericRel {
                Aggregation => {
                    attributes: { attributes.clone() },
                    group_by: { group_by.clone() },
                    from: { f(from).await },
                },
                Projection => {
                    attributes: { attributes.clone() },
                    from: { f(from).await },
                },
                Selection => {
                    from: { f(from).await },
                    where_: { where_.clone() },
                },
                Offset => {
                    offset: { offset.clone() },
                    from: { f(from).await },
                },
                Limit => {
                    number_rows: { number_rows.clone() },
                    from: { f(from).await },
                },
                OrderBy => {
                    order: { order.clone() },
                    by: { by.clone() },
                    from: { f(from).await },
                },
                Join => {
                    left: { f(left).await },
                    right: { f(right).await },
                    operator: { operator.clone() },
                },
                Set => {
                    operator: { operator.clone() },
                    left: { f(left).await },
                    right: { f(right).await },
                },
                Distinct => {
                    from: { f(from).await },
                },
                WithAlias => {
                    from: { f(from).await },
                    alias: { alias.clone() },
                },
                #[unnamed] Table => {
                    context_key: { context_key.clone() },
                },
            }
        )
    }
    pub async fn map_expressions_async<'a, F, Fut, EE>(&'a self, f: &'a F) -> GenericRel<EE, C>
    where
        F: Fn(&'a E) -> Fut,
        Fut: Future<Output = EE> + Send + 'a,
    {
        map_variants!(
            self as GenericRel {
                Aggregation => {
                    attributes: { join_all(attributes.iter().map(|elt| f(elt))).await },
                    group_by: { join_all(group_by.iter().map(|elt| f(elt))).await },
                    from: { from.clone() },
                },
                Projection => {
                    attributes: { join_all(attributes.iter().map(|elt| f(elt))).await },
                    from: { from.clone() },
                },
                Selection => {
                    from: { from.clone() },
                    where_: { f(where_).await },
                },
                Offset => {
                    offset: { f(offset).await },
                    from: { from.clone() },
                },
                Limit => {
                    number_rows: { f(number_rows).await },
                    from: { from.clone() },
                },
                OrderBy => {
                    order: { order.clone() },
                    by: { join_all(by.iter().map(|elt| f(elt))).await },
                    from: { from.clone() },
                },
                Join => {
                    left: { left.clone() },
                    right: { right.clone() },
                    operator: { operator.map_expressions_async(f).await },
                },
                Set => {
                    operator: { operator.clone() },
                    left: { left.clone() },
                    right: { right.clone() },
                },
                Distinct => {
                    from: { from.clone() },
                },
                WithAlias => {
                    from: { from.clone() },
                    alias: { alias.clone() },
                },
                #[unnamed] Table => {
                    context_key: { context_key.clone() },
                },
            }
        )
    }
    pub fn map_expressions<O, F: Fn(&E) -> O>(&self, f: &F) -> GenericRel<O, C> {
        map_variants!(
            self as GenericRel {
                Aggregation => {
                    attributes: { attributes.iter().map(f).collect() },
                    group_by: { group_by.iter().map(f).collect() },
                    from: { from.clone() },
                },
                Projection => {
                    attributes: { attributes.iter().map(f).collect() },
                    from: { from.clone() },
                },
                Selection => {
                    from: { from.clone() },
                    where_: { f(where_) },
                },
                Offset => {
                    offset: { f(offset) },
                    from: { from.clone() },
                },
                Limit => {
                    number_rows: { f(number_rows) },
                    from: { from.clone() },
                },
                OrderBy => {
                    order: { order.clone() },
                    by: { by.iter().map(f).collect() },
                    from: { from.clone() },
                },
                Join => {
                    left: { left.clone() },
                    right: { right.clone() },
                    operator: { operator.map_expressions(f) },
                },
                Set => {
                    operator: { operator.clone() },
                    left: { left.clone() },
                    right: { right.clone() },
                },
                Distinct => {
                    from: { from.clone() },
                },
                WithAlias => {
                    from: { from.clone() },
                    alias: { alias.clone() },
                },
                #[unnamed] Table => {
                    context_key: { context_key.clone() },
                },
            }
        )
    }
}

impl<B, E> RelT<B, E> {
    pub fn is_leaf(&self) -> bool {
        match &self.root {
            GenericRel::Table(..) => true,
            _ => false,
        }
    }
}

/// A rule for rebasing a relation tree. Note that rebasing a relation tree
/// means also rebasing all the wrapped expression trees.
#[async_trait]
pub trait RebaseRel<Meta, ExprMeta>: Send + Sync {
    /// The type of the new (after rebase) expression tree labels
    type NewExprMeta: ExprTryComplete + Send + Sync + Clone;
    /// The type of the new (after rebase) relation tree labels
    type NewMeta: RelTryComplete<ExprT<Self::NewExprMeta>> + Send + Sync + Clone;
    /// The type of the associated rebase rule for wrapped expression trees
    type AsRebaseExpr: RebaseExpr<ExprMeta, NewMeta = Self::NewExprMeta>;
    /// To what do we want to rebase the given node `at`? If returns `None`, we do not
    /// touch it and, instead, the recursion carries on.
    async fn maybe_at(
        &self,
        at: &RelT<Meta, ExprT<ExprMeta>>,
    ) -> Option<RelT<Self::NewMeta, ExprT<Self::NewExprMeta>>>;
    /// Derive the associated expression rebase rule. This could depend on the
    /// current relation node type (`root`).
    async fn as_rebase_expr(
        &self,
        root: &GenericRel<ExprT<ExprMeta>, RelT<Self::NewMeta, ExprT<Self::NewExprMeta>>>,
    ) -> Self::AsRebaseExpr;
}

#[async_trait]
impl RebaseRel<TableMeta, ExprMeta> for Context<TableMeta> {
    type NewExprMeta = ExprMeta;
    type NewMeta = TableMeta;
    type AsRebaseExpr = Context<ExprMeta>;
    async fn maybe_at(
        &self,
        at: &RelT<TableMeta, ExprT<ExprMeta>>,
    ) -> Option<RelT<Self::NewMeta, ExprT<Self::NewExprMeta>>> {
        match &at.root {
            GenericRel::Table(Table(context_key)) => Some(RelT {
                root: GenericRel::Table(Table(context_key.clone())),
                board: self
                    .get(context_key)
                    .map(|v| v.clone())
                    .map_err(|e| e.into_table_error()),
            }),
            _ => None,
        }
    }
    async fn as_rebase_expr(
        &self,
        root: &GenericRel<ExprT<ExprMeta>, RelT<Self::NewMeta, ExprT<Self::NewExprMeta>>>,
    ) -> Self::AsRebaseExpr {
        let mut inherited_ctx = Context::new();
        root.map(&mut |child| {
            if let Ok(ctx) = child.try_to_context() {
                inherited_ctx.extend(ctx);
            }
        });
        inherited_ctx
    }
}

impl<Meta, ExprMeta> RelT<Meta, ExprT<ExprMeta>>
where
    Meta: Send + Sync + Clone,
    ExprMeta: Send + Sync + Clone,
{
    /// Asynchronously rebase the relation tree using the rebase rule `R`.
    /// This can be used to replay the tree against a different context,
    /// or to recursively transform subtrees.
    pub fn rebase<'a, R>(
        &'a self,
        r: &'a R,
    ) -> impl Future<Output = RelT<R::NewMeta, ExprT<R::NewExprMeta>>> + Send + 'a
    where
        R: RebaseRel<Meta, ExprMeta> + Send + Sync,
    {
        async move {
            if let Some(new_base) = r.maybe_at(self).await {
                new_base
            } else {
                let rebased_root = self
                    .root
                    .map_async(async move |child: &Arc<Self>| child.rebase(r).await)
                    .await;

                let inherited = r.as_rebase_expr(&rebased_root).await;

                let inherited_ref = &inherited;

                let rebased_root = rebased_root
                    .map_expressions_async(&async move |expr: &ExprT<ExprMeta>| {
                        expr.rebase(inherited_ref).await
                    })
                    .await;

                RelT::from(rebased_root)
            }
        }
        .boxed()
    }
}

impl<B, E> RelT<B, E>
where
    B: RelTryComplete<E>,
    E: Clone,
{
    pub fn from<T: Into<GenericRel<E, Self>>>(t: T) -> Self {
        let rel: GenericRel<E, Self> = t.into();
        let root = rel.map_owned(&mut |c| Arc::new(c));
        Self::from_wrapped(root)
    }
    pub fn from_wrapped<T: Into<GenericRel<E, Arc<Self>>>>(t: T) -> Self {
        let root: GenericRel<E, Arc<Self>> = t.into();
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

impl<B, E> GenericRelTree<E> for RelT<B, E>
where
    B: Clone,
    E: Clone,
{
    fn as_ref(&self) -> GenericRel<E, &Self> {
        self.root.map(&mut |n| n.as_ref())
    }
    fn into_inner(self) -> GenericRel<E, Self> {
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
