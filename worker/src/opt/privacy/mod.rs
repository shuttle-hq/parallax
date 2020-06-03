use crate::common::*;
use futures::future;
use futures::try_join;

use crate::node::Access;

use super::{
    Aggregation, As, BinaryOp, BinaryOperator, Column, Context, ContextKey, Distinct, Expr,
    ExprMeta, ExprRepr, ExprT, Function, FunctionName, GenericRel, Join, Limit, Named, Offset,
    OrderBy, Projection, RebaseExpr, RebaseRel, RelRepr, RelT, Relation, Replace, Repr, Selection,
    Set, SetOperator, Table, TableMeta, Taint, ToContext, ValidateError, ValidateResult, WithAlias,
};

derive_rel_repr! {
    over FlexExprMeta derive
    #[derive(Serialize, Deserialize, Debug, Clone,)]
    pub struct FlexTableMeta {
        pub row_count: RowCount,
        pub primary: PrimaryMeta,
        pub columns: Context<FlexExprMeta>,
    }
}

impl Repr for FlexTableMeta {
    type ExprRepr = FlexExprMeta;
    type RelRepr = Self;
    fn to_inner_context<E>(root: GenericRel<&E, &Self::RelRepr>) -> Context<Self::ExprRepr> {
        let mut ctx = Context::new();
        root.map(&mut |child| ctx.extend(child.columns.iter().cloned()));
        ctx
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RowCount(Option<u64>);

impl From<u64> for RowCount {
    fn from(row_count: u64) -> Self {
        Self(Some(row_count))
    }
}

impl<'a, 'b> std::ops::Add<&'a RowCount> for &'b RowCount {
    type Output = RowCount;
    fn add(self, rhs: &'a RowCount) -> Self::Output {
        if let Some(lhs) = self.0.as_ref() {
            if let Some(rhs) = rhs.0.as_ref() {
                return RowCount(Some(lhs + rhs));
            }
        }
        RowCount(None)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrimaryMeta {
    pub key: ContextKey,
    pub maximum_frequency: MaximumFrequency,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq)]
pub struct MaximumFrequency(pub Option<u64>);

impl From<u64> for MaximumFrequency {
    fn from(maximum_frequency: u64) -> Self {
        Self(Some(maximum_frequency))
    }
}

derive_expr_repr! {
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq,)]
    pub struct FlexExprMeta {
        pub domain_sensitivity: DomainSensitivity,
        pub taint: Taint,
    }
}

impl Named for FlexExprMeta {}

impl<E> RelRepr<E> for RowCount {
    fn dot(node: GenericRel<&E, &Self>) -> ValidateResult<Self> {
        match node {
            GenericRel::Table(..) => Err(ValidateError::Internal(
                "tried to complete a column".to_string(),
            )),
            GenericRel::Set(Set {
                operator: SetOperator::Union,
                left,
                right,
            }) => Ok(left + right),
            GenericRel::Projection(Projection { from, .. })
            | GenericRel::Distinct(Distinct { from, .. })
            | GenericRel::WithAlias(WithAlias { from, .. }) => Ok(from.clone()),
            _ => Ok(Self(None)),
        }
    }
}

impl<E> RelRepr<E> for MaximumFrequency {
    fn dot(node: GenericRel<&E, &Self>) -> ValidateResult<Self> {
        match node {
            GenericRel::Table(..) => Err(ValidateError::Internal(
                "tried to complete a column".to_string(),
            )),
            GenericRel::Selection(Selection { from, .. })
            | GenericRel::Aggregation(Aggregation { from, .. })
            | GenericRel::Projection(Projection { from, .. })
            | GenericRel::Distinct(Distinct { from, .. })
            | GenericRel::WithAlias(WithAlias { from, .. })
            | GenericRel::Offset(Offset { from, .. })
            | GenericRel::Limit(Limit { from, .. })
            | GenericRel::OrderBy(OrderBy { from, .. }) => Ok(from.clone()),
            GenericRel::Join(Join { .. }) => Ok(Self(None)),
            GenericRel::Set(Set { left, right, .. }) => {
                if let Some(left) = left.0.as_ref() {
                    if let Some(right) = right.0.as_ref() {
                        return Ok(Self(Some(std::cmp::max(*left, *right))));
                    }
                }
                Ok(Self(None))
            }
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Copy, Clone)]
pub enum Domain {
    Discrete { max: i64, min: i64, step: u64 },
    Continuous { min: f64, max: f64 },
    Opaque,
}

impl Domain {
    fn to_sensitivity(&self) -> Sensitivity {
        let sensitivity = match self {
            Self::Discrete { max, min, .. } => Some((max - min) as f64),
            Self::Continuous { max, min, .. } => Some(max - min),
            Self::Opaque => None,
        };
        Sensitivity(sensitivity)
    }
}

impl Default for Domain {
    fn default() -> Self {
        Self::Opaque
    }
}

impl std::fmt::Display for Domain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Discrete { max, min, step } => write!(f, "discrete({}:{}:{})", min, step, max),
            Self::Continuous { max, min } => write!(f, "continuous({}:{})", min, max),
            Self::Opaque => write!(f, "opaque"),
        }
    }
}

impl ExprRepr for Domain {
    fn dot(node: Expr<&Self>) -> ValidateResult<Self> {
        match node {
            Expr::Column(Column(ck)) => Err(ValidateError::Internal(format!(
                "tried to complete a column {}",
                ck
            ))),
            Expr::As(As { expr, .. }) => Ok(expr.clone()),
            Expr::IsNull(..) | Expr::IsNotNull(..) | Expr::InList(..) | Expr::Between(..) => {
                Ok(Self::Discrete {
                    max: 0,
                    min: 1,
                    step: 1,
                })
            }
            Expr::Function(Function { name, mut args, .. }) => {
                // TODO: refactor Function struct to not have to validate this
                // every time.
                let arg = args.pop().ok_or(ValidateError::Expected(
                    "function to have argument".to_string(),
                ))?;
                match name {
                    FunctionName::Max | FunctionName::Min => Ok(arg.clone()),
                    FunctionName::Avg | FunctionName::StdDev => match arg {
                        Self::Discrete { min, max, .. } => Ok(Self::Continuous {
                            min: *min as f64,
                            max: *max as f64,
                        }),
                        _ => Ok(arg.clone()),
                    },
                    FunctionName::Count | FunctionName::Sum => Ok(Self::Opaque),
                    FunctionName::Concat => Ok(Self::Opaque),
                }
            }
            Expr::Replace(Replace { with, .. }) => Ok(with.clone()),
            Expr::BinaryOp(BinaryOp { left, op, right }) => {
                // TODO: this sometimes can be inferred for the arithmetic operations
                Ok(Self::Opaque)
            }
            _ => Ok(Self::Opaque), // default is Ok(opaque)
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Copy, Clone)]
pub struct Sensitivity(pub Option<f64>);

#[derive(Debug, PartialEq, Serialize, Deserialize, Copy, Clone)]
pub struct DomainSensitivity {
    pub domain: Domain,
    pub sensitivity: Sensitivity,
    pub maximum_frequency: MaximumFrequency,
}

impl ExprRepr for DomainSensitivity {
    fn dot(node: Expr<&Self>) -> ValidateResult<Self> {
        let domain = Domain::dot(node.map(&mut |child| &child.domain))?;

        let mut maximum_frequency = None;
        node.map(&mut |child| {
            if let Some(child_mf) = child.maximum_frequency.0.as_ref() {
                if let Some(mf) = maximum_frequency.as_mut() {
                    *mf = max(*mf, *child_mf);
                } else {
                    maximum_frequency = Some(*child_mf);
                }
            }
        });

        let sensitivity = match node.map(&mut |child| &child.sensitivity) {
            Expr::Column(..) => {
                return Err(ValidateError::Internal(
                    "tried to complete a column".to_string(),
                ));
            }
            Expr::IsNull(..) | Expr::IsNotNull(..) | Expr::InList(..) | Expr::Between(..) => {
                Some(1.)
            }
            Expr::Literal(..) => Some(0.),
            Expr::BinaryOp(BinaryOp { op, .. }) => match op {
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulus => None,
                _ => Some(1.),
            },
            Expr::Replace(Replace { with, .. }) => with.0.clone(),
            Expr::Function(Function { name, mut args, .. }) => {
                // TODO
                let arg = args.pop().ok_or(ValidateError::Expected(
                    "function to have argument".to_string(),
                ))?;
                match name {
                    FunctionName::Count => {
                        if maximum_frequency.is_some() {
                            Some(1.)
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            _ => None,
        };

        Ok(Self {
            domain,
            sensitivity: Sensitivity(sensitivity),
            maximum_frequency: MaximumFrequency(maximum_frequency),
        })
    }
}

impl<E> RelRepr<E> for PrimaryMeta {
    fn dot(node: GenericRel<&E, &Self>) -> ValidateResult<Self> {
        let mut children = HashSet::new();
        node.map(&mut |child| children.insert(&child.key));
        if children.len() != 1 {
            Err(ValidateError::Wip(format!(
                "relation operations involving more than one primary key: {:?}",
                children
            )))
        } else {
            let key = children.drain().next().unwrap().clone();
            let maximum_frequency =
                MaximumFrequency::dot(node.map_owned(&mut |child| &child.maximum_frequency))?;
            Ok(Self {
                key,
                maximum_frequency,
            })
        }
    }
}

pub struct FlexTableMetaGetter<'a, A> {
    pub primary: String,
    pub access: &'a A,
}

impl<'a, A> FlexTableMetaGetter<'a, A>
where
    A: Access,
{
    pub async fn get(&self, table_meta: &TableMeta) -> crate::Result<FlexTableMeta> {
        let loc = table_meta
            .loc
            .as_ref()
            .ok_or(Error::new("table does not have a backend location"))?;

        let backend = self.access.backend(loc)?;

        let source = table_meta
            .source
            .as_ref()
            .ok_or(Error::new("table does not have a source key"))?;

        debug!("acquiring a probe for {}", source);
        let probe = backend.probe(source).await?;

        let primary_key = &ContextKey::with_name(&self.primary);
        debug!(
            "acquiring maximum_frequency and row_count for {}",
            primary_key
        );
        let (maximum_frequency, row_count) =
            try_join!(probe.maximum_frequency(&primary_key), probe.row_count())?;

        let primary = PrimaryMeta {
            key: primary_key.clone(),
            maximum_frequency: maximum_frequency.clone(),
        };

        let access = self.access;
        let probe_ref = &probe;

        let mut columns = Context::new();
        let columns_with_meta: Vec<(_, crate::Result<_>)> =
            future::join_all(table_meta.columns.iter().map(
                async move |(column, previous_meta)| {
                    let meta = try {
                        debug!("acquiring domain for {}", column);
                        let domain = probe_ref.domain(column).await?;

                        let maximum_frequency = maximum_frequency.clone();
                        let sensitivity = domain.to_sensitivity();

                        let domain_sensitivity = DomainSensitivity {
                            domain,
                            sensitivity,
                            maximum_frequency,
                        };

                        let taint = column.matches(primary_key).into();

                        FlexExprMeta {
                            domain_sensitivity,
                            taint,
                        }
                    };
                    (column.clone(), meta)
                },
            ))
            .await;

        for (column, meta) in columns_with_meta {
            match meta {
                Ok(meta) => columns.insert(column, meta),
                Err(err) => {
                    debug!("failed to build flex meta for \"{}\": {}", column, err);
                    return Err(err);
                }
            }
        }

        Ok(FlexTableMeta {
            row_count,
            primary,
            columns,
        })
    }
    pub async fn rebase(&self, rel: &Relation<TableMeta>) -> Relation<FlexTableMeta> {
        rebase_closure!(rel => TableMeta -> FlexTableMeta {
            async move |table_meta, ctx_key| {
                self.get(table_meta)
                    .await
                    .map_err(|err| ValidateError::Internal(err.to_string()))
            }
        })
        .await
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use crate::node::state::tests::read_manifest;
    use crate::node::tests::mk_node;
    use crate::opt::Taint;
    use crate::opt::{validate::Validator, Relation};

    use tokio::runtime::Runtime;

    fn rebase_query_to_flex_meta(query_str: &str) -> Relation<FlexTableMeta> {
        let random_scope = uuid::Uuid::new_v4().to_simple().to_string();
        let access = Arc::new(mk_node(&random_scope));
        for resource in read_manifest().into_iter() {
            access.create_resource(resource).unwrap();
        }
        Runtime::new().unwrap().block_on(async move {
            let ctx = access.context().await.unwrap();
            let validator = Validator::new(&ctx);
            let rel_t = validator.validate_str(query_str).unwrap();
            let getter = FlexTableMetaGetter {
                access: &access,
                primary: "person_id".to_string(),
            };
            let getter_ref = &getter;
            let rebase_fut = rebase_closure!(
                rel_t => TableMeta -> FlexTableMeta {
                    async move |table_meta, key| {
                        getter_ref.get(table_meta)
                            .await
                            .map_err(|e| ValidateError::Wip("generic err".to_string()))
                    }
                }
            );
            rebase_fut.await
        })
    }

    #[test]
    fn simple_rebase_to_flex_query() {
        let rebased = rebase_query_to_flex_meta(
            "SELECT COUNT(DISTINCT person_id), MAX(year_of_birth) FROM patient_data.person",
        );
        let flex_meta = rebased.board.unwrap();
        assert_eq!(flex_meta.row_count.0, Some(2326856));
        assert_eq!(flex_meta.primary.maximum_frequency.0, Some(1));
        assert_eq!(
            *flex_meta
                .columns
                .get(&ContextKey::with_name("f0_"))
                .unwrap(),
            FlexExprMeta {
                domain_sensitivity: DomainSensitivity {
                    domain: Domain::Opaque,
                    sensitivity: Sensitivity(Some(1.0,),),
                    maximum_frequency: MaximumFrequency(Some(1,),),
                },
                taint: true.into(),
            }
        );
        assert_eq!(
            *flex_meta
                .columns
                .get(&ContextKey::with_name("f1_"))
                .unwrap(),
            FlexExprMeta {
                domain_sensitivity: DomainSensitivity {
                    domain: Domain::Discrete {
                        max: 1983,
                        min: 1909,
                        step: 1
                    },
                    sensitivity: Sensitivity(None),
                    maximum_frequency: MaximumFrequency(Some(1))
                },
                taint: Taint(false)
            }
        );
    }
}
