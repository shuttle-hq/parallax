use crate::common::{Policy as ApiPolicy, *};
use crate::Result;

use super::Access;

use crate::backends::Backend;
use crate::job::Job;
use crate::node::resource::{BlockStore, BlockStoreExt, RedisBlockStore, SharedScope};
use crate::node::state::Scope;
use crate::opt::validate::Validate;
use crate::opt::{Context, ContextKey, DataType, ExprMeta, Policy, PolicyBinding, TableMeta};
use crate::Opt;

lazy_static! {
    static ref DATA_RE: regex::Regex =
        { regex::Regex::new("(?P<resource>[^\\[\\]]+)\\[\"(?P<data>[^\"]*)\"\\]$").unwrap() };
}

pub fn create_resource<A: Access>(access: &A, resource: Resource) -> Result<Resource> {
    let resource_ty = resource.block_type().map_err(|e| ScopeError::from(e))?;
    let block = access.resource(&resource_ty)?;
    let mut lock = block.write().map_err(|e| ScopeError::from(e))?;
    if !lock.is_some() {
        *lock = Some(resource.clone());
        block.push(lock);
        Ok(resource)
    } else {
        block.abort(lock);
        Err(ScopeError::already_exists(&resource_ty).into())
    }
}

pub fn update_resource<A: Access>(
    access: &A,
    resource_ty: &BlockType,
    resource: Resource,
) -> Result<Resource> {
    let existing_ty = resource.block_type().map_err(|e| ScopeError::from(e))?;
    if existing_ty != *resource_ty {
        let err = ScopeError {
            kind: ScopeErrorKind::Unknown as i32,
            description: "cannot change a resource's name at the \
                          moment. Instead consider deleting and \
                          creating at the new resource"
                .to_string(),
            ..Default::default()
        };
        Err(err.into())
    } else {
        let block = access.resource(&resource_ty)?;
        let mut lock = block.write().map_err(|e| ScopeError::from(e))?;
        if !lock.is_none() {
            *lock = Some(resource.clone());
            block.push(lock);
            Ok(resource)
        } else {
            block.abort(lock);
            Err(ScopeError::not_found(&resource_ty).into())
        }
    }
}

pub fn delete_resource<A: Access>(access: &A, resource_ty: &BlockType) -> Result<()> {
    let block = access.resource(&resource_ty)?;
    let mut lock = block.write().map_err(|e| ScopeError::from(e))?;
    if !lock.is_none() {
        *lock = None;
        block.push(lock);
        Ok(())
    } else {
        block.abort(lock);
        Err(ScopeError::not_found(&resource_ty).into())
    }
}

pub async fn context<A: Access>(access: &A) -> Result<Context<TableMeta>> {
    let datasets = access.resources(&block_type!("resource"."dataset"."*"))?;
    let mut ctx = Context::new();
    for resource in datasets {
        let dataset = resource.try_downcast::<Dataset>()?;
        let dataset_name = dataset.name.clone();
        for data in dataset.data.into_iter() {
            // FIXME efficiency, needs nested scopes
            let ((backend_block, _), data_in_context) = {
                let mut captures_iter = DATA_RE.captures_iter(&data);
                let (resource, data) = captures_iter
                    .next()
                    .and_then(|captures| {
                        let resource = captures.name("resource")?.as_str();
                        let data = captures.name("data")?.as_str();
                        Some((resource, data))
                    })
                    .ok_or(ScopeError {
                        kind: ScopeErrorKind::BadSplat as i32,
                        source: data.to_string(),
                        ..Default::default()
                    })?;
                let bt = BlockType::parse::<Resource>(resource)?;
                let ck = ContextKey::from_str(data).map_err(|e| e.into_error())?;
                (bt, ck)
            };

            let mut table_meta = access
                .backend(&backend_block)?
                .probe(&data_in_context)
                .await?
                .to_meta()
                .await?;

            table_meta.loc = Some(backend_block.clone());

            let context_key =
                ContextKey::with_name(data_in_context.name()).and_prefix(&dataset_name);
            ctx.insert(context_key, table_meta);
        }
    }

    Ok(ctx)
}

pub fn user<A: Access>(access: &A, user_id: &str) -> Result<Option<User>> {
    let maybe_user = access
        .resource(&block_type!("resource"."user".user_id))?
        .clone_inner()?;
    maybe_user
        .map(|user| user.try_into().map_err(|e| Error::from(e)))
        .transpose()
}

pub fn group<A: Access>(access: &A, group_id: &str) -> Result<Option<Group>> {
    let maybe_group = access
        .resource(&block_type!("resource"."group".group_id))?
        .clone_inner()?;
    maybe_group
        .map(|group| group.try_into().map_err(|e| Error::from(e)))
        .transpose()
}

pub fn groups_for_user<A: Access>(access: &A, user_id: &str) -> Result<Vec<BlockType>> {
    debug!("getting groups for {}", user_id);
    let user_type = block_type!("resource"."user".user_id);
    let user = access.user(user_id)?.ok_or(ScopeError::not_found("user"))?;

    let mut groups = Vec::new();
    for group in access.resources(&block_type!("resource"."group"."*"))? {
        for member in group.clone().try_downcast::<Group>()?.members.into_iter() {
            // FIXME: should be checking tokenstream is empty
            if user_type == BlockType::parse::<Resource>(&member)?.0 {
                groups.push(group.block_type()?);
            }
        }
    }

    Ok(groups)
}

pub fn policies_for_group<A: Access>(access: &A, audience: &str) -> Result<Context<PolicyBinding>> {
    let mut context = Context::new();
    for resource in access.resources(&block_type!("resource"."dataset"."*"))? {
        let dataset = resource.try_downcast::<Dataset>()?;
        let ctx = dataset_as_policy_context(dataset, audience)?;
        context.extend(ctx);
    }
    Ok(context)
}

fn dataset_as_policy_context(
    dataset: Dataset,
    audience: &str,
) -> std::result::Result<Context<PolicyBinding>, ScopeError> {
    let audience_ty = block_type!("resource"."group".audience);
    let dataset_name = &dataset.name;

    let policies_scope = dataset
        .policies
        .into_iter()
        .map(|policy| Ok((policy.block_type()?, Policy(policy.try_unwrap()?))))
        .collect::<std::result::Result<Scope<Policy>, ScopeError>>()?;

    let mut policy_bindings = HashMap::new();

    for binding in dataset.policy_bindings.into_iter() {
        let name = &binding.name;

        let target_audience = binding
            .groups
            .into_iter()
            .map(|aud| Ok(BlockType::parse::<Resource>(&aud)?.0))
            .collect::<std::result::Result<HashSet<_>, TypeError>>()?;

        if target_audience.contains(&audience_ty) {
            for policy_ref in binding.policies.into_iter() {
                let policy_type = BlockType::parse::<ApiPolicy>(&policy_ref)?.0;

                let policy = policies_scope
                    .get(&policy_type)
                    .ok_or(ScopeError::not_found(&policy_type))?
                    .clone();

                let binding = PolicyBinding {
                    policy,
                    priority: binding.priority,
                };

                policy_bindings
                    .entry(policy_type)
                    .and_modify(|existing: &mut PolicyBinding| {
                        existing.priority = max(existing.priority, binding.priority);
                    })
                    .or_insert(binding);
            }
        }
    }

    let as_context = policy_bindings
        .into_iter()
        .map(|(pt, binding)| {
            ContextKey::from_iter(pt.into_iter())
                .map(|ck| (ck.and_prefix(dataset_name), binding))
                .unwrap()
        })
        .collect();

    Ok(as_context)
}
