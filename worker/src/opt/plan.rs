use crate::common::*;

use super::{
    rel::GenericRelTree, Context, ContextKey, Rel, RelT, Table, TableMeta, ValidateError,
    ValidateResult,
};
use crate::opt::ContextError;

fn children_locations(node: &Rel<Step>) -> ValidateResult<HashSet<&BlockType>> {
    let mut locs = HashSet::new();
    node.map(&mut |child| {
        child
            .rel_t
            .board
            .as_ref()
            .map_err(|e| e.clone())
            .and_then(|b| {
                let loc = b
                    .loc
                    .as_ref()
                    .ok_or(ValidateError::Insufficient("location".to_string()))?;
                Ok(locs.insert(loc))
            })
    })
    .into_result()?;
    Ok(locs)
}

fn generate_promise_key() -> ContextKey {
    let name = Uuid::new_v4().to_simple().to_string();
    ContextKey::with_name(&name)
        .and_prefix("ephemeral")
        .and_prefix("parallax_internal")
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Step {
    pub rel_t: RelT,
    pub ctx: Context<TableMeta>,
    pub promise: ContextKey,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PhysicalPlan {
    /// the steps that need to be executed
    pub steps: Vec<Step>,
    /// the context for ephemeral tables created in the process.
    /// These will need migration between backends
    pub ctx: Context<TableMeta>,
}

pub struct PhysicalPlanner<'a> {
    ctx: &'a Context<TableMeta>,
    rel_t: RelT,
}

impl<'a> PhysicalPlanner<'a> {
    pub fn new(ctx: &'a Context<TableMeta>, rel_t: RelT) -> Self {
        Self { ctx, rel_t }
    }

    pub fn into_plan(self) -> ValidateResult<PhysicalPlan> {
        let mut plan = Vec::<Step>::new();
        let mut job_context: Context<TableMeta> = Context::new();

        let ctx = self.ctx;

        let last_step = self.rel_t.try_fold(&mut |node: Rel<Step>| {
            match node {
                Rel::Table(Table(key)) => {
                    let table_meta = ctx.get_table(&key)?;

                    if table_meta.loc.is_none() {
                        return Err(ValidateError::Insufficient(key.to_string()));
                    }
                    let mut ctx = Context::new();
                    ctx.insert(key.clone(), table_meta.clone());
                    let step = Step {
                        rel_t: RelT {
                            root: Rel::Table(Table(key.clone())),
                            board: Ok(table_meta.clone()),
                        },
                        ctx,
                        promise: key.clone(),
                    };
                    Ok(step)
                }
                // Not a leaf
                _ => {
                    let locs = children_locations(&node)?;

                    if locs.len() == 1 {
                        let mut ctx = Context::new();
                        let rel = node.map_owned(&mut |child| {
                            ctx.extend(child.ctx);
                            child.rel_t
                        });
                        let promise = generate_promise_key();
                        let step = Step {
                            rel_t: RelT::from(rel),
                            ctx,
                            promise,
                        };
                        Ok(step)
                    } else {
                        // cannot fail because children_locations would have failed
                        let chosen_loc = (*locs.iter().next().unwrap()).clone();
                        let mut new_ctx = Context::new();
                        node.map_owned(&mut |step: Step| {
                            let step_promise = &step.promise;
                            let step_meta = step.rel_t.board.as_ref().map_err(|e| e.clone())?;
                            job_context.insert(step_promise.clone(), step_meta.clone());
                            let mut board = step_meta.clone();
                            board.loc = Some(chosen_loc.clone());

                            let node = RelT {
                                root: Rel::Table(Table(step_promise.clone())),
                                board: Ok(board),
                            };
                            new_ctx.insert(step_promise.clone(), step_meta.clone());
                            plan.push(step);
                            Ok(node)
                        })
                        .into_result()
                        .map(|rel| {
                            let promise = generate_promise_key();
                            Step {
                                rel_t: RelT::from(rel),
                                ctx: new_ctx,
                                promise,
                            }
                        })
                    }
                }
            }
        })?;

        plan.push(last_step);

        Ok(PhysicalPlan {
            steps: plan,
            ctx: job_context,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::opt::{validate::Validator, DataType, ExprMeta};

    fn get_context() -> Context<TableMeta> {
        let mut ctx = Context::new();
        ctx.insert(
            "my_dataset.table_A".parse().unwrap(),
            TableMeta {
                columns: {
                    let mut ctx = Context::new();
                    ctx.insert(
                        "column_1".parse().unwrap(),
                        ExprMeta {
                            ty: DataType::Integer,
                            ..Default::default()
                        },
                    );
                    ctx.insert(
                        "index".parse().unwrap(),
                        ExprMeta {
                            ty: DataType::String,
                            ..Default::default()
                        },
                    );

                    ctx
                },
                loc: Some(block_type!("resource"."backend"."big_query"."my_bq_dataset")),
                ..Default::default()
            },
        );

        ctx.insert(
            "my_other_dataset.table_B".parse().unwrap(),
            TableMeta {
                columns: {
                    let mut ctx = Context::new();
                    ctx.insert(
                        "column_1".parse().unwrap(),
                        ExprMeta {
                            ty: DataType::Integer,
                            ..Default::default()
                        },
                    );
                    ctx.insert(
                        "index".parse().unwrap(),
                        ExprMeta {
                            ty: DataType::String,
                            ..Default::default()
                        },
                    );

                    ctx
                },
                loc: Some(block_type!("resource"."backend"."drill"."my_drill_dataset")),
                ..Default::default()
            },
        );

        ctx
    }

    const QUERY: &'static str = "\
    SELECT a.column_1 + b.column_1 \
    FROM my_dataset.table_A as a \
    JOIN my_other_dataset.table_B as b \
    ON a.index = b.index";

    #[test]
    fn physically_plan() {
        let ctx = get_context();

        let rel_t = Validator::new(&ctx).validate_str(QUERY).unwrap();

        let physical_plan = PhysicalPlanner::new(&ctx, rel_t).into_plan().unwrap();
    }
}
