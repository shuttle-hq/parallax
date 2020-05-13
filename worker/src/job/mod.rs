use crate::common::*;
use crate::node::{Access, Node};
use crate::Result;

use crate::opt::plan::{PhysicalPlan, PhysicalPlanner, Step};
use crate::opt::transform;
use crate::opt::validate::Validator;
use crate::opt::{
    Context, ContextKey, RelT, RelTransformer, TableMeta, Transformed, ValidateError,
};

pub(crate) mod processor;
pub(crate) use processor::Processor;

use crate::common::{
    Block, BlockType, Job as ApiJob, JobState as ApiJobState, JobStatus as ApiJobStatus,
    TokenStream,
};

type Sha256Bytes = String;

fn closures_match_or_err(left: &Sha256Bytes, right: &Sha256Bytes) -> Result<()> {
    if left != right {
        debug!("closure mismatch: expected {} but got {}", left, right);

        let description = format!(
            "a closure has changed: expected '{}' but got '{}'",
            left, right
        );

        let err = ScopeError {
            kind: ScopeErrorKind::Changed as i32,
            description,
            ..Default::default()
        };

        Err(err.into())
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    source: ContextKey,
    loc: BlockType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryInitialisedStage {
    query: String,
}

impl QueryInitialisedStage {
    async fn validate<A: Access>(self, access: &A) -> Result<QueryValidatedStage> {
        debug!("initialised -> validated");

        let ctx = access.context().await?;
        let closure = ctx.sha256();

        debug!("validating {} in {}", self.query, closure);
        let validator = Validator::new(&ctx);
        let validated = validator
            .validate_str(&self.query)
            .map_err(|e| e.into_error())?;

        match validated.board.as_ref() {
            Ok(_) => {
                debug!("validated {:?} successfully", validated);
                Ok(QueryValidatedStage { closure, validated })
            }
            Err(err) => {
                debug!("validation failed {}", err);
                Err(err.clone().into_error().into())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryValidatedStage {
    closure: Sha256Bytes,
    validated: RelT,
}

impl QueryValidatedStage {
    async fn optimize<A: Access>(self, access: &A) -> Result<QueryOptimizedStage> {
        debug!("validate -> optimize");

        let ctx = access.context().await?;
        let closure = ctx.sha256();

        closures_match_or_err(&self.closure, &closure)?;

        let default_group = access.default_group();
        let policies = access.policies_for_group(default_group)?;
        let policies_closure = policies.sha256();

        let audience = block_type!("resource"."group".default_group);

        debug!(
            "optimizing (closure: {}, policies: {})",
            closure, policies_closure
        );
        let transformer = RelTransformer::new(&policies, &audience, access);
        let optimized = transformer
            .transform_rel(&self.validated)
            .await
            .or_else(|err| match err {
                transform::Error::NoMatch => Ok(Transformed::default(self.validated)),
                transform::Error::Validate(err) => Err(err),
            })
            .map_err(|e| e.into_error())?;

        match optimized.root.board.as_ref() {
            Ok(board) => {
                debug!("optimization lead to a valid tree");
                if board.audience.contains(&audience) {
                    debug!("found a compliant tree, authorizing");
                    access.expend_to_budget(optimized.cost)?;

                    Ok(QueryOptimizedStage {
                        closure,
                        policies_closure,
                        optimized: optimized.root,
                    })
                } else {
                    debug!("could not find a compliant tree");
                    let err = AccessError {
                        kind: AccessErrorKind::Forbidden as i32,
                        culprit: access.who_am_i().to_string(),
                        description: "could not authorize query".to_string(),
                        ..Default::default()
                    };
                    Err(err.into())
                }
            }
            Err(err) => {
                debug!("optimization lead to an invalid tree {:?}", err);
                Err(err.clone().into_error().into())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOptimizedStage {
    closure: Sha256Bytes,
    policies_closure: Sha256Bytes,
    optimized: RelT,
}

impl QueryOptimizedStage {
    async fn plan<A: Access>(self, access: &A) -> Result<QueryPlannedStage> {
        let ctx = access.context().await?;
        let closure = ctx.sha256();

        closures_match_or_err(&self.closure, &closure)?;

        let plan = PhysicalPlanner::new(&ctx, self.optimized)
            .into_plan()
            .map_err(|e| e.into_error())?;

        Ok(QueryPlannedStage { closure, plan })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlannedStage {
    pub closure: Sha256Bytes,
    pub plan: PhysicalPlan,
}

impl QueryPlannedStage {
    async fn execute<A: Access>(self, access: &A) -> Result<QueryDoneStage> {
        debug!("planned -> done");

        let mut current_asset = None;
        for (
            idx,
            Step {
                rel_t,
                ctx,
                promise,
            },
        ) in self.plan.steps.into_iter().enumerate()
        {
            debug!(
                "executing a step (tree: {:?}, closure: {}, promise: {})",
                rel_t, self.closure, promise
            );

            let step_board = rel_t.board.as_ref().map_err(|e| e.clone().into_error())?;

            let step_loc = match step_board.loc.as_ref() {
                Some(loc) => loc,
                None => {
                    let desc = format!(
                        "step cannot execute as it is missing a \
                         backend field to send it to (step: {}, output: {})",
                        idx, promise
                    );
                    let err = ValidateError::Insufficient(desc).into_error();
                    return Err(err.into());
                }
            };

            // TODO: Add migration here.
            current_asset = Some(Asset {
                loc: step_loc.clone(),
                source: promise.clone(),
            });

            access
                .backend(step_loc)?
                .compute(Step {
                    rel_t,
                    ctx,
                    promise,
                })
                .await?;
        }

        if let Some(asset) = current_asset {
            Ok(QueryDoneStage { asset })
        } else {
            Err(Error::new("job had no output"))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDoneStage {
    asset: Asset,
}

/// The stages of a 'query' job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryStages {
    Initialised(QueryInitialisedStage),
    Validated(QueryValidatedStage),
    Optimized(QueryOptimizedStage),
    Planned(QueryPlannedStage),
    Done(QueryDoneStage),
}

impl QueryStages {
    fn to_proto_state(&self) -> ApiJobState {
        match self {
            Self::Initialised(..) => ApiJobState::Pending,
            Self::Validated(..) | Self::Optimized(..) | Self::Planned(..) => ApiJobState::Running,
            Self::Done(..) => ApiJobState::Done,
        }
    }

    async fn advance<A: Access>(self, access: &A) -> Result<Self> {
        let next = match self {
            Self::Initialised(init) => Self::Validated(init.validate(access).await?),
            Self::Validated(valid) => Self::Optimized(valid.optimize(access).await?),
            Self::Optimized(optim) => Self::Planned(optim.plan(access).await?),
            Self::Planned(planned) => Self::Done(planned.execute(access).await?),
            Self::Done(done) => Self::Done(done),
        };
        Ok(next)
    }

    fn is_done(&self) -> bool {
        match self {
            Self::Done(..) => true,
            _ => false,
        }
    }
}

/// FSM for job processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobStage {
    Query(QueryStages),
}

impl JobStage {
    fn to_proto_state(&self) -> ApiJobState {
        match self {
            Self::Query(query) => query.to_proto_state(),
        }
    }

    pub async fn advance<A: Access>(self, access: &A) -> Result<Self> {
        match self {
            Self::Query(query) => Ok(Self::Query(query.advance(access).await?)),
        }
    }

    fn is_done(&self) -> bool {
        match self {
            Self::Query(query) => query.is_done(),
        }
    }
}

pub type JobState = Result<JobStage>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Foreman {
    pub(self) peer: BlockType,
    pub(self) id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// The id at which this job can be found
    pub(self) id: String,
    /// The creation timestamp of this job
    pub(self) timestamp: DateTime<Utc>,
    /// The current state of this job
    pub(self) state: JobState,
    /// The user on behalf of which this job has been inserted
    pub(self) user: String,
    /// The foreman of this job
    pub(self) foreman: Option<Foreman>,
}

impl Block for Job {
    fn block_type(&self) -> std::result::Result<BlockType, parallax_api::swamp::TypeError> {
        Ok(block_type!("job".(&self.id)))
    }

    fn parse_block_type(
        stream: TokenStream,
    ) -> std::result::Result<(BlockType, TokenStream), parallax_api::swamp::TypeError> {
        stream.parse("job")?.take()?.done()
    }
}

impl Job {
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn to_proto(&self) -> ApiJob {
        let status = self.to_proto_status();

        ApiJob {
            id: self.id.clone(),
            user: self.user.clone(),
            query: String::new(), // FIXME
            status: Some(status),
            timestamp: self.timestamp.to_string(),
        }
    }

    pub fn into_proto(self) -> ApiJob {
        self.to_proto()
    }

    pub fn user_matches(&self, id: &str) -> bool {
        &self.user == id
    }

    fn to_proto_status(&self) -> ApiJobStatus {
        let mut state = ApiJobState::default();
        let mut final_error = None;

        match self.state.as_ref() {
            Ok(stage) => {
                state = stage.to_proto_state();
            }
            Err(err) => {
                final_error = Some(err.clone());
                state = ApiJobState::Done;
            }
        };

        // TODO
        //let errors: Vec<Error> = self.logs
        //    .iter()
        //    .cloned()
        //    .collect();
        let errors = Vec::new();

        ApiJobStatus {
            state: state as i32,
            final_error,
            errors,
        }
    }

    pub fn is_done(&self) -> bool {
        self.state
            .as_ref()
            .map(|state| state.is_done())
            .ok()
            .unwrap_or(true)
    }
}
