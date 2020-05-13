use crate::common::*;

use super::{
    expr::As, Aggregation, AudienceBoard, Between, BinaryOp, BinaryOperator, Column, Context,
    ContextKey, Distribution, Expr, ExprMeta, ExprT, ExprTree, Function, FunctionName, GenericRel,
    GenericRelTree, Hash, HashAlgorithm, Literal, LiteralValue, Noisy, Projection, Rel, RelT,
    Selection, Table, TableMeta, ToContext, TryToContext, ValidateError,
};
use crate::node::Access;
use crate::opt::{ContextError, RebaseRel};

use super::privacy::*;

/// Small helper to figure out if a given context key matches any of the given field patterns
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

#[derive(Debug, Clone)]
pub struct Policy(pub policy::Policy);

pub struct Costly<T> {
    root: T,
    cost: f64,
}

impl<T> From<T> for Costly<T> {
    fn from(root: T) -> Self {
        Self { root, cost: 0. }
    }
}

impl ExprTransform for WhitelistPolicy {
    fn transform_expr(&self, expr: &ExprT) -> Result<Costly<ExprT>, Error> {
        match expr.as_ref() {
            Expr::Column(Column(context_key)) => {
                if matches_in(self.fields.iter(), &context_key)? {
                    Ok(expr.clone().into())
                } else {
                    Err(Error::NoMatch)
                }
            }
            _ => Err(Error::NoMatch),
        }
    }
}

impl ExprTransform for HashPolicy {
    fn transform_expr(&self, expr: &ExprT) -> Result<Costly<ExprT>, Error> {
        match expr.as_ref() {
            Expr::Column(Column(context_key)) => {
                if matches_in(self.fields.iter(), &context_key)? {
                    Ok(ExprT::from(Expr::Hash(Hash {
                        algo: HashAlgorithm::default(),
                        expr: expr.clone(),
                        salt: self.salt.clone(),
                    }))
                    .into())
                } else {
                    Err(Error::NoMatch)
                }
            }
            _ => Err(Error::NoMatch),
        }
    }
}

impl ExprTransform for ObfuscatePolicy {
    fn transform_expr(&self, expr: &ExprT) -> Result<Costly<ExprT>, Error> {
        match expr.as_ref() {
            Expr::Column(Column(context_key)) => {
                if matches_in(self.fields.iter(), &context_key)? {
                    let expr = ExprT::from(Expr::Literal(Literal(LiteralValue::Null)));
                    let alias = context_key.name().to_string();
                    Ok(ExprT::from(Expr::As(As { expr, alias })).into())
                } else {
                    Err(Error::NoMatch)
                }
            }
            _ => Err(Error::NoMatch),
        }
    }
}

impl ExprTransform for Policy {
    fn transform_expr(&self, expr: &ExprT) -> Result<Costly<ExprT>, Error> {
        match &self.0 {
            policy::Policy::Whitelist(whitelist) => whitelist.transform_expr(expr),
            policy::Policy::Hash(hash) => hash.transform_expr(expr),
            policy::Policy::Obfuscate(obfuscate) => obfuscate.transform_expr(expr),
            _ => Err(Error::NoMatch),
        }
    }
}

#[async_trait]
impl RelTransform for DifferentialPrivacyPolicy {
    async fn transform_rel<A: Access>(
        &self,
        rel: &RelT,
        access: &A,
    ) -> Result<Costly<RelT>, Error> {
        match rel.as_ref() {
            GenericRel::Aggregation(Aggregation {
                attributes,
                group_by,
                from,
            }) => {
                // FIXME: This could be optimized
                let getter = FlexTableMetaGetter {
                    primary: self.entity.clone(),
                    access,
                };
                let flex = getter.rebase(rel).await;

                if let Err(_) = flex.board.as_ref() {
                    return Err(Error::NoMatch);
                }

                let (flex_attributes, flex_group_by, flex_from) = match flex.as_ref() {
                    GenericRel::Aggregation(Aggregation {
                        attributes,
                        group_by,
                        from,
                    }) => (attributes, group_by, from),
                    _ => unreachable!(),
                };

                let mut factor = 1.;
                let mut grouping_keys = HashSet::new();
                for (expr, flex_expr) in group_by.iter().zip(flex_group_by.iter()) {
                    if let Expr::Column(Column(column_key)) = expr.as_ref() {
                        grouping_keys.insert(column_key);
                        let col_maximum_frequency = flex_expr
                            .board
                            .as_ref()
                            .map_err(|e| e.clone())?
                            .domain_sensitivity
                            .maximum_frequency
                            .0
                            .ok_or(Error::NoMatch)?;
                        factor *= col_maximum_frequency as f64;
                    } else {
                        return Err(Error::NoMatch);
                    }
                    if flex_expr.board.as_ref().map_err(|e| e.clone())?.taint.0 {
                        return Err(Error::NoMatch);
                    }
                }

                let bucket_alias = "__bucket_count";
                let bucket_key = ContextKey::with_name(bucket_alias);

                let maximum_frequency = flex_from
                    .board
                    .as_ref()
                    .map_err(|e| e.clone())?
                    .primary
                    .maximum_frequency
                    .0
                    .ok_or(Error::NoMatch)?;

                let threshold = (self.bucket_size * maximum_frequency) as i64;

                let one = ExprT::from(Expr::Literal(Literal(LiteralValue::Long(1))));

                // this cost is per row
                let mut cost = 0.;
                let mut new_attributes = Vec::new();
                let mut projection_attributes = Vec::new();
                for (i, (expr, flex_expr)) in
                    attributes.iter().zip(flex_attributes.iter()).enumerate()
                {
                    match expr.as_ref() {
                        Expr::Column(Column(column_key)) => {
                            if !grouping_keys.contains(&column_key) {
                                return Err(Error::NoMatch);
                            }
                            new_attributes.push(ExprT::from(Expr::As(As {
                                expr: expr.clone(),
                                alias: column_key.name().to_string(),
                            })));
                            projection_attributes.push(expr.clone());
                        }
                        Expr::Function(Function {
                            name,
                            args,
                            distinct,
                        }) => {
                            // assuming function is aggregation
                            let board = flex_expr.board.as_ref().map_err(|e| e.clone())?;
                            let sensitivity = board
                                .domain_sensitivity
                                .sensitivity
                                .0
                                .ok_or(Error::NoMatch)?;

                            let distribution = Distribution::Laplace {
                                mean: 0.,
                                variance: sensitivity / self.epsilon,
                            };

                            cost += self.epsilon;

                            let alias = format!("f{}_", i);

                            let new_expr = ExprT::from(Expr::As(As {
                                expr: ExprT::from(Expr::Noisy(Noisy {
                                    expr: expr.clone(),
                                    distribution,
                                })),
                                alias: alias.clone(),
                            }));
                            new_attributes.push(new_expr);

                            let alias_as_col =
                                ExprT::from(Expr::Column(Column(ContextKey::with_name(&alias))));
                            projection_attributes.push(alias_as_col);
                        }
                        _ => return Err(Error::NoMatch),
                    }
                }

                new_attributes.push(ExprT::from(Expr::As(As {
                    expr: ExprT::from(Expr::Noisy(Noisy {
                        expr: ExprT::from(Expr::Function(Function {
                            name: FunctionName::Count,
                            args: vec![one.clone()],
                            distinct: false,
                        })),
                        distribution: Distribution::Laplace {
                            mean: 0.,
                            variance: 1. / self.epsilon,
                        },
                    })),
                    alias: bucket_alias.to_string(),
                })));

                let noised_root = RelT::from(GenericRel::Aggregation(Aggregation {
                    attributes: new_attributes,
                    group_by: group_by.clone(),
                    from: from.clone(),
                }));

                let where_bucket_count = ExprT::from(Expr::BinaryOp(BinaryOp {
                    op: BinaryOperator::Gt,
                    left: ExprT::from(Expr::Column(Column(bucket_key))),
                    right: { ExprT::from(Expr::Literal(Literal(LiteralValue::Long(threshold)))) },
                }));

                let new_root = RelT::from(GenericRel::Projection(Projection {
                    from: RelT::from(GenericRel::Selection(Selection {
                        from: noised_root,
                        where_: where_bucket_count,
                    })),
                    attributes: projection_attributes,
                }));

                let ctx = access.context().await.unwrap();
                let new_root = RebaseRel::<'_, TableMeta>::rebase(&ctx, &new_root).await; // repair it

                Ok(Costly {
                    root: new_root,
                    cost,
                })
            }
            _ => Err(Error::NoMatch),
        }
    }
}

#[async_trait]
impl RelTransform for Policy {
    async fn transform_rel<A: Access>(
        &self,
        rel: &RelT,
        access: &A,
    ) -> Result<Costly<RelT>, Error> {
        match &self.0 {
            policy::Policy::DifferentialPrivacy(differential_privacy) => {
                differential_privacy.transform_rel(rel, access).await
            }
            _ => Err(Error::NoMatch),
        }
    }
}

#[derive(derive_more::From, Debug)]
pub enum Error {
    NoMatch,
    Validate(ValidateError),
}

pub trait ExprTransform {
    fn transform_expr(&self, expr: &ExprT) -> Result<Costly<ExprT>, Error>;
}

#[async_trait]
pub trait RelTransform {
    async fn transform_rel<A: Access>(&self, rel: &RelT, access: &A)
        -> Result<Costly<RelT>, Error>;
}

#[derive(Clone, Debug)]
pub struct PolicyBinding {
    pub policies: Vec<Policy>,
    pub priority: u64,
    pub budget: Option<PolicyBudget>,
}

impl PolicyBinding {
    fn is_in_budget(&self, proposed: f64) -> bool {
        self.budget
            .as_ref()
            .map(|PolicyBudget { maximum, used, .. }| used + proposed <= *maximum)
            .unwrap_or(true)
    }
}

pub struct RelTransformer<'a, A> {
    bindings: &'a Context<PolicyBinding>,
    audience: &'a BlockType,
    access: &'a A,
}

impl<'a, A> RelTransformer<'a, A>
where
    A: Access,
{
    pub fn new(
        bindings: &'a Context<PolicyBinding>,
        audience: &'a BlockType,
        access: &'a A,
    ) -> Self {
        debug!(
            "initializing relation transformer for {} with bindings={:?}",
            audience, bindings
        );
        Self {
            bindings,
            audience,
            access,
        }
    }

    /// Filter the policy bindings that apply to the `context_key`
    fn filter_bindings<'b>(&'b self, context_key: &'b ContextKey) -> Context<&'a PolicyBinding> {
        debug!("sifting policies for {}", context_key);
        self.bindings
            .iter()
            .filter_map(move |(key, binding)| {
                if key.prefix_matches(context_key) {
                    Some((key.clone(), binding))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn transform_rel<'b>(
        &'b self,
        rel_t: &'b RelT,
    ) -> Pin<Box<dyn Future<Output = Result<Transformed<RelT>, Error>> + Send + 'b>> {
        async move {
            let unraveled = rel_t.root.map(&mut |child| child.as_ref());

            let proposed = match unraveled {
                Rel::Projection(Projection {
                    mut attributes,
                    from:
                        RelT {
                            root: Rel::Table(Table(context_key)),
                            board,
                        },
                }) => {
                    debug!("potential expr leaf policy condition met");
                    let from = RelT {
                        root: Rel::Table(Table(context_key.clone())),
                        board: board.clone(),
                    };

                    let bindings = self.filter_bindings(context_key);
                    debug!("bindings filtered to {:?}", bindings);

                    let mut cost = HashMap::new();
                    let mut priority = 0;
                    let expr_transformer = ExprTransformer::new(&bindings, &self.audience);
                    for expr_t in attributes.iter_mut() {
                        match expr_transformer.transform_expr(expr_t) {
                            Ok(transformed) => {
                                debug!("successfully transformed expression");
                                transformed.add_to(&mut cost);
                                *expr_t = transformed.root;
                                priority = max(priority, transformed.priority);
                            }
                            Err(Error::NoMatch) => {}
                            Err(err) => return Err(err),
                        }
                    }
                    let root = RelT::from(Rel::Projection(Projection { attributes, from }));
                    debug!("rebuilt leaf relation node {:?}", root);

                    let audience = root
                        .board
                        .as_ref()
                        .map(|board| &board.audience)
                        .map_err(|e| Error::Validate(e.clone()))?;
                    debug!(
                        "after transformation of expression, audience: {:?}",
                        audience
                    );
                    if audience.contains(&self.audience) {
                        vec![Transformed {
                            root,
                            cost,
                            priority,
                        }]
                    } else {
                        vec![]
                    }
                }
                _ => {
                    // there might be a need for filtering of policies here,
                    // or to force fully qualifying entities in the policy setup
                    let mut candidates = Vec::new();
                    for (key, binding) in self.bindings.iter() {
                        for policy in binding.policies.iter() {
                            match policy.transform_rel(rel_t, self.access).await {
                                Ok(Costly { mut root, cost }) => {
                                    root.board
                                        .as_mut()
                                        .map(|board| board.audience.insert(self.audience.clone()))
                                        .map_err(|e| Error::Validate(e.clone()))?;
                                    let transformed =
                                        Transformed::new(root, key, cost, binding.priority);
                                    candidates.push(transformed);
                                }
                                Err(Error::NoMatch) => {}
                                Err(err) => return Err(err),
                            }
                        }
                    }
                    candidates
                }
            };

            if let Some(best) = Transformed::best_candidate(proposed) {
                debug!("best candidate for relation: {:?}", best);
                Ok(best)
            } else {
                debug!("no candidate for relation at this level");
                if rel_t.is_leaf() {
                    debug!("leaf relation attained, no match");
                    return Err(Error::NoMatch);
                }

                let state = Mutex::new((HashMap::new(), 0u64));
                let state_ref = &state;
                let root = RelT::from(
                    rel_t
                        .root
                        .map_async(async move |child| {
                            self.transform_rel(child).await.map(|transformed| {
                                let mut state = state_ref.lock().unwrap();
                                transformed.add_to(&mut state.0);
                                state.1 = max(state.1, transformed.priority);
                                transformed.root
                            })
                        })
                        .await
                        .into_result()?,
                );
                let state_ = state.lock().unwrap();
                let transformed = Transformed {
                    root,
                    cost: state_.0.clone(),
                    priority: state_.1,
                };
                debug!("from level below, got best relation tree {:?}", transformed);
                Ok(transformed)
            }
        }
        .boxed()
    }
}

pub struct ExprTransformer<'a> {
    bindings: &'a Context<&'a PolicyBinding>,
    audience: &'a BlockType,
}

impl<'a> ExprTransformer<'a> {
    fn new(bindings: &'a Context<&'a PolicyBinding>, audience: &'a BlockType) -> Self {
        Self { bindings, audience }
    }
    fn transform_expr(&self, expr_t: &ExprT) -> Result<Transformed<ExprT>, Error> {
        let mut proposed = Vec::new();
        for (key, binding) in self.bindings.iter() {
            let priority = binding.priority;
            for policy in binding.policies.iter() {
                match policy.transform_expr(expr_t) {
                    Ok(Costly { mut root, cost }) => {
                        root.board
                            .as_mut()
                            .map(|board| {
                                board.audience.insert(self.audience.clone());
                            })
                            .map_err(|e| Error::Validate(e.clone()))?;

                        let transformed = Transformed::new(root, key, cost, priority);
                        proposed.push(transformed);
                    }
                    Err(Error::NoMatch) => {}
                    Err(err) => return Err(err),
                }
            }
        }
        if let Some(best) = Transformed::best_candidate(proposed) {
            // select the best strategy
            Ok(best)
        } else {
            // no match so far, let's try deeper
            if expr_t.is_leaf() {
                return Err(Error::NoMatch);
            }
            let mut cost = HashMap::new();
            let mut priority = 0;
            let root = ExprT::from(
                expr_t
                    .root
                    .map(&mut |child| {
                        self.transform_expr(child).map(|transformed| {
                            transformed.add_to(&mut cost);
                            priority = max(priority, transformed.priority);
                            transformed.root
                        })
                    })
                    .into_result()?,
            );
            Ok(Transformed {
                root,
                cost,
                priority,
            })
        }
    }
}

#[derive(Debug, Clone)]
pub struct Transformed<T> {
    pub root: T,
    pub cost: HashMap<ContextKey, f64>,
    pub priority: u64,
}

impl<T> Transformed<T> {
    pub fn default(root: T) -> Self {
        Self {
            root,
            cost: HashMap::new(),
            priority: 0,
        }
    }
    fn new(root: T, binding_key: &ContextKey, cost: f64, priority: u64) -> Self {
        Self {
            root,
            cost: {
                let mut cost_ = HashMap::new();
                cost_.insert(binding_key.clone(), cost);
                cost_
            },
            priority,
        }
    }
    pub fn into_inner(self) -> T {
        self.root
    }
    fn best_candidate<I>(iter: I) -> Option<Self>
    where
        I: IntoIterator<Item = Self>,
    {
        let proposed: Vec<_> = iter.into_iter().collect();
        let highest = proposed
            .iter()
            .max_by(|l, r| l.priority.cmp(&r.priority))
            .map(|highest| highest.priority)?;
        let candidates = proposed
            .into_iter()
            .filter(|t| t.priority == highest)
            .collect::<Vec<_>>();
        let best = candidates
            .into_iter()
            .min_by(|l, r| l.total_cost().partial_cmp(&r.total_cost()).unwrap())
            .unwrap();
        Some(best)
    }
    fn total_cost(&self) -> f64 {
        self.cost.values().sum()
    }
    fn add_to(&self, costs: &mut HashMap<ContextKey, f64>) {
        for (key, cost) in self.cost.iter() {
            *costs.entry(key.clone()).or_default() += cost;
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use crate::node::state::tests::read_manifest;
    use crate::node::tests::mk_node;
    use crate::opt::validate::Validator;
    use tokio::runtime::Runtime;

    use parallax_api::block_type;

    fn test_transform_for(query: &str) -> Transformed<RelT> {
        let random_scope = uuid::Uuid::new_v4().to_simple().to_string();
        let access = Arc::new(mk_node(&random_scope));
        for resource in read_manifest().into_iter() {
            access.create_resource(resource).unwrap();
        }
        Runtime::new().unwrap().block_on(async {
            let ctx = access.context().await.unwrap();
            let validator = Validator::new(&ctx);
            let policies = access.policies_for_group("group1").unwrap();
            let rel_t = validator.validate_str(query).unwrap();
            let audience = block_type!("resource"."group"."group1");
            let transformer = RelTransformer::new(&policies, &audience, &access);
            let rel_t = transformer
                .transform_rel(&rel_t)
                .await
                .or_else(|error| match error {
                    super::Error::NoMatch => Ok(Transformed::default(rel_t)),
                    super::Error::Validate(err) => Err(err),
                })
                .unwrap();
            rel_t
        })
    }

    #[test]
    fn transform_blocked() {
        let rel_t = test_transform_for(
            "\
            SELECT city FROM yelp.business
            ",
        )
        .into_inner();
        let table_meta = rel_t.board.unwrap();
        assert!(table_meta.audience.is_empty())
    }

    #[test]
    fn transform_whitelist() {
        let rel_t = test_transform_for(
            "\
            SELECT business_id FROM yelp.business
            ",
        )
        .into_inner();
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
        )
        .into_inner();

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
        )
        .into_inner();

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

    #[test]
    fn transform_diff_priv() {
        let rel_t = test_transform_for(
            "\
            SELECT business_id, COUNT(funny) \
            FROM yelp.review \
            GROUP BY business_id
            ",
        );
        println!("{:#?}", rel_t);
    }
}
