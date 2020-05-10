use crate::common::*;

use crate::node::Access;

use super::{
    Aggregation, Context, ContextKey, Distinct, Domain, Expr, ExprMeta, ExprRepr, ExprT,
    GenericRel, Join, Limit, Named, Offset, OrderBy, Projection, RebaseExpr, RebaseRel, RelRepr,
    RelT, Relation, Repr, Selection, Set, SetOperator, Table, TableMeta, ToContext, ValidateError,
    ValidateResult, WithAlias,
};

derive_rel_repr! {
    over FlexExprMeta derive
    #[derive(Serialize, Deserialize, Debug, Clone,)]
    pub struct FlexTableMeta {
        row_count: RowCount,
        primary: PrimaryMeta,
        columns: Context<FlexExprMeta>,
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
    key: ContextKey,
    maximum_frequency: MaximumFrequency,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MaximumFrequency(Option<u64>);

impl From<u64> for MaximumFrequency {
    fn from(maximum_frequency: u64) -> Self {
        Self(Some(maximum_frequency))
    }
}

derive_expr_repr! {
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq,)]
    pub struct FlexExprMeta {
        domain: Domain,
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

pub struct FlexTableMetaGetter<A> {
    primary: String,
    access: A,
}

impl<A> FlexTableMetaGetter<A>
where
    A: Access,
{
    async fn get(&self, table_meta: &TableMeta) -> crate::Result<FlexTableMeta> {
        let loc = table_meta
            .loc
            .as_ref()
            .ok_or(Error::new("table does not have a backend location"))?;

        let backend = self.access.backend(loc)?;

        let source = table_meta
            .source
            .as_ref()
            .ok_or(Error::new("table does not have a source key"))?;

        let probe = backend.probe(source).await?;

        let mut columns = Context::new();
        for column in table_meta.columns.keys() {
            let domain = probe.domain(column).await?;
            let meta = FlexExprMeta { domain };
            columns.insert(column.clone(), meta);
        }

        let primary_key = ContextKey::with_name(&self.primary);
        let maximum_frequency = probe.maximum_frequency(&primary_key).await?;
        let primary = PrimaryMeta {
            key: primary_key,
            maximum_frequency,
        };

        let row_count = probe.row_count().await?;

        Ok(FlexTableMeta {
            row_count,
            primary,
            columns,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use crate::node::state::tests::read_manifest;
    use crate::node::tests::mk_node;
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
                access,
                primary: "user_id".to_string(),
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
            "SELECT COUNT(DISTINCT user_id), MAX(stars) FROM yelp.review",
        );
        let flex_meta = rebased.board.unwrap();
        assert_eq!(
            *flex_meta
                .columns
                .get(&ContextKey::with_name("f1_"))
                .unwrap(),
            FlexExprMeta {
                domain: Domain::Continuous { min: 1., max: 5. }
            }
        );
        assert_eq!(flex_meta.row_count.0, Some(6685900));
        assert_eq!(flex_meta.primary.maximum_frequency.0, Some(4129));
    }
}
