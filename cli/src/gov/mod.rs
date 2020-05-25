use colored::Colorize;

use anyhow::{Error, Result};

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use tonic::Request;

use parallax_api::{
    client::Client, Block, BlockType, CreateResourceRequest, DeleteResourceRequest,
    ListResourcesRequest, ListResourcesResponse, Resource, UpdateResourceRequest,
};

type Scope = HashMap<BlockType, Resource>;

#[derive(Default, Clone, PartialEq, Debug)]
pub struct Plan {
    create: Vec<Resource>,
    update: Vec<Diff>,
    delete: Vec<Resource>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct Diff {
    old: Resource,
    new: Resource,
}

impl std::fmt::Display for Diff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let old_str = format_resource(&self.old, Operation::Update)?;
        let new_str = format_resource(&self.new, Operation::Update)?;
        write!(f, "{}\n=>\n{}", old_str, new_str)?;
        Ok(())
    }
}

enum Operation {
    Create,
    Update,
    Delete,
}

fn format_resource(
    resource: &Resource,
    op: Operation,
) -> std::result::Result<String, std::fmt::Error> {
    let mut raw_str = serde_yaml::to_string(resource).unwrap();

    let break_index = raw_str.find("\n").unwrap_or(0);

    // remove the first line which is just '---'
    let resource_str = raw_str.drain(break_index..).collect();

    let resource_ty = resource.block_type().map_err(|_| std::fmt::Error)?;

    let resource_op_str = match op {
        Operation::Create => format!("+ {}", resource_ty).green(),
        Operation::Update => format!("~ {}", resource_ty).yellow(),
        Operation::Delete => format!("- {}", resource_ty).red(),
    };

    Ok(format!(
        "\n  {}\n{}\n",
        resource_op_str,
        indent(resource_str)
    ))
}

fn indent(s: String) -> String {
    s.replace("\n", "\n\t")
}

impl std::fmt::Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_noop() {
            write!(
                f,
                "{}",
                "\n  No changes. State up to date.\n".bright_green().bold()
            )?;
            return Ok(());
        }

        write!(f, "\nA state mutation plan has been generated below.\n")?;
        write!(
            f,
            "State mutation operations are indicated by the following symbols:\n"
        )?;
        write!(f, "  {} create\n", "+".green().bold())?;
        write!(f, "  {} update\n", "~".yellow().bold())?;
        write!(f, "  {} delete\n", "-".red().bold())?;

        for resource in &self.create {
            let resource_str = format_resource(resource, Operation::Create)?;
            writeln!(f, "{}", resource_str)?;
        }

        for resource in &self.delete {
            let resource_str = format_resource(resource, Operation::Delete)?;
            writeln!(f, "{}", resource_str)?;
        }

        for diff in &self.update {
            let resource_str = format!("{}", diff);
            writeln!(f, "{}", resource_str)?;
        }

        let plan_summary = writeln!(
            f,
            "{} {} to add, {} to update, {} to delete.\n",
            "Plan:".bold(),
            self.create.len(),
            self.update.len(),
            self.delete.len()
        );

        Ok(())
    }
}

impl Plan {
    pub fn new(from: Scope, to: Scope) -> Result<Self> {
        let mut plan = Plan::default();

        for (name, new_resource) in to.iter() {
            match from.get(name) {
                Some(old_resource) => {
                    if old_resource != new_resource {
                        plan.update(old_resource.clone(), new_resource.clone())
                    }
                }
                None => {
                    plan.create(new_resource.clone());
                }
            }
        }

        for (name, old_resource) in from.iter() {
            match to.get(name) {
                None => {
                    plan.delete(old_resource.clone());
                }
                _ => { /* This case is already taken care of above */ }
            }
        }

        Ok(plan)
    }
    pub async fn execute(self, client: &mut Client) -> Result<()> {
        for create in self.create.into_iter() {
            let req = Request::new(CreateResourceRequest {
                resource: Some(create),
            });
            client.create_resource(req).await?;
        }

        for update in self.update.into_iter() {
            let old_ty = update.old.block_type()?;
            let req = Request::new(UpdateResourceRequest {
                resource_name: old_ty.to_string(),
                resource: Some(update.new),
            });
            client.update_resource(req).await?;
        }

        for delete in self.delete.into_iter() {
            let old_ty = delete.block_type()?;
            let req = Request::new(DeleteResourceRequest {
                resource_name: old_ty.to_string(),
            });
            client.delete_resource(req).await?;
        }

        println!("{}", "\n  Apply completed successfully.\n".green().bold());

        Ok(())
    }
    pub fn is_noop(&self) -> bool {
        self.create.is_empty() && self.update.is_empty() && self.delete.is_empty()
    }
    fn create(&mut self, resource: Resource) {
        self.create.push(resource);
    }

    fn update(&mut self, old: Resource, new: Resource) {
        self.update.push(Diff { old, new });
    }

    fn delete(&mut self, resource: Resource) {
        self.delete.push(resource)
    }
}

/* FIXME: reintroduce in due time
struct LockWrapper {
    lock: LockId,
}

impl LockWrapper {
    async fn acquire(governor_client: &mut GovernorClient<Channel>) -> Result<Self, GovernorError> {
        let lock = governor_client
            .acquire_lock(Empty {})
            .await
            .map_err(|e| {
                GovernorError::LockError(format!(
                    "Could not acquire lock with error: {}",
                    e.to_string()
                ))
            })?
            .into_inner();

        Ok(Self { lock })
    }

    async fn release(
        &self,
        governor_client: &mut GovernorClient<Channel>,
    ) -> Result<(), GovernorError> {
        governor_client
            .release_lock(self.lock.clone())
            .await
            .map_err(|e| {
                GovernorError::LockError(format!(
                    "Could not release lock with error: {}",
                    e.to_string()
                ))
            })?;
        Ok(())
    }
}
*/

pub fn read_workspace_dir(workspace_dir: &Path) -> Result<Scope> {
    let split: Vec<_> = walkdir::WalkDir::new(workspace_dir)
        .into_iter()
        .filter_map(|file| {
            let file = file.ok()?;
            let is_yaml = file.file_name().to_str()?.ends_with(".yaml");
            if is_yaml {
                Some(file.into_path())
            } else {
                None
            }
        })
        .map(|path| {
            let mut buf = String::new();
            File::open(&path).and_then(|mut f| f.read_to_string(&mut buf))?;
            let resources: Vec<Resource> = serde_yaml::from_str(&buf)
                .map_err(|e| Error::msg(format!("Error in {}: {}", path.as_path().display(), e)))?;
            Ok((resources, path))
        })
        .collect::<Result<Vec<_>>>()?;

    let together: Vec<(Resource, PathBuf)> = split
        .into_iter()
        .flat_map(|(resources, path): (Vec<Resource>, PathBuf)| {
            resources
                .into_iter()
                .map(move |resource| (resource, path.clone()))
        })
        .collect();

    let mut builder = HashMap::<BlockType, (Resource, PathBuf)>::new();
    for (resource, path) in together.into_iter() {
        let ty = resource.block_type()?;
        if builder.contains_key(&ty) {
            let (_, snd) = builder.get(&ty).unwrap();
            let err_msg = format!(
                "resource `{}` is declared twice: in {} and in {}",
                ty.to_string(),
                path.to_str().unwrap_or("UNKNOWN"),
                snd.to_str().unwrap_or("UNKNOWN")
            );
            return Err(Error::msg(err_msg));
        } else {
            builder.insert(ty, (resource, path));
        }
    }

    let scope = builder.into_iter().map(|(k, (v, _))| (k, v)).collect();

    Ok(scope)
}

pub async fn get_remote_scope(client: &mut Client) -> Result<Scope> {
    let req = Request::new(ListResourcesRequest {
        pattern: "resource.*".to_string(),
    });
    let ListResourcesResponse { resources } = client.list_resources(req).await?.into_inner();

    let mut scope = HashMap::new();
    for resource in resources.into_iter() {
        let ty = resource.block_type()?;
        if scope.contains_key(&ty) {
            let err_msg = format!(
                "invalid state received from remote: found duplicate of {}",
                ty
            );
            return Err(Error::msg(err_msg));
        } else {
            scope.insert(ty, resource);
        }
    }

    Ok(scope)
}
