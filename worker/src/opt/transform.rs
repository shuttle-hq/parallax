use crate::common::*;

use super::{
    expr::As, AudienceBoard, Between, Column, Context, ContextKey, Expr, ExprMeta, ExprT, ExprTree,
    GenericRel, GenericRelTree, Hash, HashAlgorithm, Literal, LiteralValue, Projection, Rel, RelT,
    RelTree, Table, TableMeta, ToContext, TryToContext, ValidateError,
};
use crate::opt::ContextError;

#[derive(Debug, Clone)]
pub struct Policy(pub policy::Policy);

fn matches_in<'a, I: IntoIterator<Item = &'a String>>(
    iter: I,
    key: &'a ContextKey,
) -> Result<bool, ValidateError> {
    for field in iter.into_iter() {
        if key.matches(&field.parse()?) {
            return Ok(true);
        }
    }
    return Ok(false);
}

impl ExprTransform for Policy {
    fn transform_expr(&self, expr: &ExprT, ctx: &Context<ExprMeta>) -> Result<ExprT, Error> {
        match expr.as_ref() {
            Expr::Column(Column(context_key)) => {
                let matches = match &self.0 {
                    policy::Policy::Whitelist(WhitelistPolicy { fields, .. })
                    | policy::Policy::Hash(HashPolicy { fields, .. })
                    | policy::Policy::Obfuscate(ObfuscatePolicy { fields, .. }) => {
                        matches_in(fields.iter(), &context_key)?
                    }
                };

                if matches {
                    match &self.0 {
                        policy::Policy::Whitelist(WhitelistPolicy { .. }) => Ok(expr.clone()),
                        policy::Policy::Hash(HashPolicy { salt, .. }) => {
                            Ok(ExprT::from(Expr::Hash(Hash {
                                algo: HashAlgorithm::default(),
                                expr: expr.clone(),
                                salt: salt.clone(),
                            })))
                        }
                        policy::Policy::Obfuscate(ObfuscatePolicy { .. }) => {
                            let expr = ExprT::from(Expr::Literal(Literal(LiteralValue::Null)));
                            let alias = context_key.name().to_string();
                            Ok(ExprT::from(Expr::As(As { expr, alias })))
                        }
                    }
                } else {
                    Err(Error::NoMatch)
                }
            }
            _ => Err(Error::NoMatch),
        }
    }
}

#[derive(derive_more::From)]
pub enum Error {
    NoMatch,
    Validate(ValidateError),
}

pub trait ExprTransform {
    fn transform_expr(&self, expr: &ExprT, ctx: &Context<ExprMeta>) -> Result<ExprT, Error>;
}

#[derive(Clone, Debug)]
pub struct PolicyBinding {
    pub policy: Policy,
    pub priority: u64,
}

pub struct RelTransformer<'a> {
    ctx: &'a Context<TableMeta>,
    bindings: &'a Context<PolicyBinding>,
    audience: &'a BlockType,
}

impl<'a> RelTransformer<'a> {
    pub fn new(
        ctx: &'a Context<TableMeta>,
        bindings: &'a Context<PolicyBinding>,
        audience: &'a BlockType,
    ) -> Self {
        Self {
            ctx,
            bindings,
            audience,
        }
    }

    fn iter_policies<'b>(
        &'b self,
        context_key: &'b ContextKey,
    ) -> impl Iterator<Item = &'b PolicyBinding> {
        /// FIXME: this is a bad solution (flattening all policies in a single context and
        /// relying on root of context key to decide which dataset it applies to
        debug!("sifting policies for {}", context_key);
        self.bindings
            .iter()
            .filter(move |(key, binding)| {
                key.root()
                    .and_then(|k| Some(k == context_key.root()?))
                    .unwrap_or(false)
            })
            .map(|(_, binding)| binding)
    }

    pub fn transform_rel(&self, rel_t: RelT) -> Result<RelT, ValidateError> {
        let unraveled = rel_t
            .clone()
            .into_inner()
            .map_owned(&mut |child| child.into_inner());
        let res = match unraveled {
            Rel::Projection(Projection {
                mut attributes,
                from: Rel::Table(Table(context_key)),
            }) => {
                debug!("potential rel leaf policy condition met");
                let table_meta = self.ctx.get_table(&context_key)?;

                let expr_ctx = table_meta.to_context();

                let mut audiences = Vec::new();

                attributes = attributes
                    .into_iter()
                    .map(|expr_t| {
                        let expr_t = self.transform_expr(expr_t, &context_key, &expr_ctx)?;
                        let audience = &expr_t.board.as_ref().map_err(|e| e.clone())?.audience;
                        debug!("after transform_expr we have audience={:?}", audience);
                        audiences.push(audience.clone());
                        Ok(expr_t)
                    })
                    .collect::<Result<_, ValidateError>>()?;

                debug!("repackaging rel leaf node");
                let mut rel_t = RelT::from(Rel::Projection(Projection {
                    attributes,
                    from: RelT {
                        root: Rel::Table(Table(context_key)),
                        board: Ok(table_meta.clone()),
                    },
                }));

                let mut new_audience = audiences
                    .pop()
                    .map(|aud| aud.clone())
                    .unwrap_or(HashSet::new());
                for audience in audiences.into_iter() {
                    new_audience = new_audience.intersection(&audience).cloned().collect();
                }

                debug!("this rel leaf node has now audience={:?}", new_audience);
                rel_t.board.as_mut().map(|mut res| {
                    res.audience = new_audience;
                });

                rel_t
            }
            _ => rel_t,
        };

        let audience = &res.board.as_ref().map_err(|e| e.clone())?.audience;
        if audience.contains(&self.audience) {
            debug!("rel target achieved");
            Ok(res)
        } else {
            debug!("rel target NOT achieved");
            if !res.is_leaf() {
                debug!("not a rel leaf, descending");
                let res = res
                    .into_inner()
                    .map_owned(&mut |child| self.transform_rel(child))
                    .into_result()?;
                Ok(RelT::from(res))
            } else {
                Ok(res)
            }
        }
    }

    fn transform_expr(
        &self,
        expr_t: ExprT,
        policy_ctx: &ContextKey,
        expr_ctx: &Context<ExprMeta>,
    ) -> Result<ExprT, ValidateError> {
        let stack = self
            .iter_policies(policy_ctx)
            .map(|PolicyBinding { policy, priority }| (policy, *priority))
            .inspect(|(pol, prior)| debug!("BINDING policy={:?} at priority={}", pol, prior))
            .collect::<Vec<_>>();
        ExprTransformer::from(expr_ctx, &self.audience)
            .transform_expr(expr_t, stack.as_slice())
            .map(|(res, _)| res)
    }
}

#[derive(Clone)]
pub struct OrderedTree<T>(Option<T>, u64);

impl<T> std::cmp::PartialEq<Self> for OrderedTree<T> {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}

impl<T> std::cmp::PartialOrd<Self> for OrderedTree<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.1.partial_cmp(&other.1)
    }
}

impl<T> OrderedTree<T> {
    fn new() -> Self {
        Self(None, 0)
    }

    fn from(tree: T, val: u64) -> Self {
        Self(Some(tree), val)
    }

    fn unwrap_or(self, t: T) -> (T, u64) {
        (self.0.unwrap_or(t), self.1)
    }

    fn replace_if_stronger(&mut self, other: Self) -> &mut Self {
        if self.0.is_some() {
            if &other > self {
                *self = other;
            }
        } else {
            *self = other;
        }
        self
    }
}

pub struct ExprTransformer<'a> {
    ctx: &'a Context<ExprMeta>,
    audience: &'a BlockType,
}

impl<'a> ExprTransformer<'a> {
    fn from(ctx: &'a Context<ExprMeta>, audience: &'a BlockType) -> Self {
        Self { ctx, audience }
    }

    fn transform_expr(
        &self,
        expr_t: ExprT,
        stack: &[(&Policy, u64)],
    ) -> Result<(ExprT, u64), ValidateError> {
        let current_audience = expr_t
            .board
            .as_ref()
            .map(|b| &b.audience)
            .map_err(|e| e.clone())?;
        debug!("at point transform_expr audience={:?}", current_audience);

        if current_audience.contains(&self.audience) {
            return Ok((expr_t, 0));
        }

        let mut remaining = Vec::new();

        let mut out = OrderedTree::new();

        // apply the policies at current depth
        for policy in stack.iter() {
            let res = policy.0.transform_expr(&expr_t, &self.ctx);
            match res {
                Ok(mut new) => {
                    debug!("for policy {:?}, match at expr node", policy);
                    if let Ok(audience) = new.board.as_mut().map(|b| &mut b.audience) {
                        audience.insert(self.audience.clone());
                    }
                    let new = OrderedTree::from(new, policy.1);
                    out.replace_if_stronger(new);
                }
                Err(Error::NoMatch) => {
                    debug!("for policy {:?}, NO match at expr node", policy);
                    remaining.push(*policy);
                }
                Err(Error::Validate(verr)) => {
                    debug!("for policy {:?}, validation error", policy);
                    return Err(verr);
                }
            }
        }

        // apply the remaining policies one step deeper, if relevant
        if !expr_t.is_leaf() {
            let mut children_priority = 0;
            let children = expr_t
                .as_ref()
                .map_owned(&mut |child| {
                    self.transform_expr(child.clone(), remaining.as_slice())
                        .map(|(tfed, priority)| {
                            children_priority = max(children_priority, priority);
                            tfed
                        })
                })
                .into_result()
                .map(|res| OrderedTree::from(ExprT::from(res), children_priority))?;

            out.replace_if_stronger(children);
        }

        Ok(out.unwrap_or(expr_t))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use crate::node::tests::{just_get_me_a_context, just_get_me_policies};
    use crate::opt::validate::Validator;

    use parallax_api::block_type;

    fn test_transform_for(query: &str) -> RelT {
        let ctx = just_get_me_a_context();
        let validator = Validator::new(&ctx);
        let policies = just_get_me_policies();
        let rel_t = validator.validate_str(query).unwrap();
        let audience = block_type!("resource"."group"."group1");
        let transformer = RelTransformer::new(&ctx, &policies, &audience);
        let rel_t = transformer.transform_rel(rel_t).unwrap();
        rel_t
    }

    #[test]
    fn transform_blocked() {
        let rel_t = test_transform_for(
            "\
            SELECT city FROM yelp.business
            ",
        );
        let table_meta = rel_t.board.unwrap();
        assert!(table_meta.audience.is_empty())
    }

    #[test]
    fn transform_whitelist() {
        let rel_t = test_transform_for(
            "\
            SELECT business_id FROM yelp.business
            ",
        );
        let table_meta = rel_t.board.unwrap();
        assert!(table_meta
            .audience
            .contains(&block_type!("resource"."group"."group1")))
    }

    use crate::opt::expr::As;

    #[test]
    fn transform_obfuscation() {
        let rel_t = test_transform_for(
            "\
            SELECT review_id FROM yelp.review
            ",
        );

        let table_meta = rel_t.board.unwrap();
        assert!(table_meta
            .audience
            .contains(&block_type!("resource"."group"."group1")));

        match rel_t.root {
            Rel::Projection(Projection { attributes, .. }) => {
                match attributes[0]
                    .as_ref()
                    .map_owned(&mut |child| child.as_ref())
                {
                    Expr::As(As {
                        expr: Expr::Literal(Literal(LiteralValue::Null)),
                        alias,
                    }) => assert_eq!(alias, "review_id".to_string()),
                    _ => panic!("`review_id` was not obfuscated"),
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn transform_hash() {
        let rel_t = test_transform_for(
            "\
            SELECT user_id FROM yelp.review
            ",
        );

        let table_meta = rel_t.board.unwrap();
        assert!(table_meta
            .audience
            .contains(&block_type!("resource"."group"."group1")));

        match rel_t.root {
            Rel::Projection(Projection { attributes, .. }) => {
                match attributes[0]
                    .as_ref()
                    .map_owned(&mut |child| child.as_ref())
                {
                    Expr::Hash(..) => {}
                    _ => panic!("`user_id` was not hashed"),
                }
            }
            _ => unreachable!(),
        }
    }
}
