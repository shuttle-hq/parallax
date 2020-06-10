#![feature(async_closure)]

use anyhow::{Error, Result};
use structopt::StructOpt;

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use csv as csv_crate;

use tonic::transport::Uri;

#[macro_use]
extern crate prettytable;

use prettytable::{Cell, Row, Table};

use dialoguer::Confirmation;

mod config;

use config::{Config, Identities, Identity, Keyring};

mod init;

use init::init;

mod gov;

mod job;

mod catalog;

use crate::job::Fetch;
use parallax_api::Job;
use parallax_api::JobState;
use std::sync::Arc;

fn with_expand_home(p: &str) -> Result<PathBuf> {
    let expanded = shellexpand::tilde(p).into_owned();
    Ok(Path::new(&expanded).to_owned())
}

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "parallax", about = "Manage Parallax resources and workflows")]
pub struct Opt {
    #[structopt(
    long,
    help = "the location of parallax home",
    env = "PARALLAX_HOME",
    default_value = "~/.config/parallax",
    parse(try_from_str = with_expand_home)
    )]
    home: PathBuf,
    #[structopt(long, help = "disable tls (NOT RECOMMENDED)")]
    disable_tls: bool,
    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(Debug, Clone, StructOpt)]
enum Command {
    #[structopt(
        about = "Initialises local identity files (used for bootstrapping a new deployment)"
    )]
    Init {
        #[structopt(
            long,
            help = "the host of the parallax deployment to target",
            default_value = "127.0.0.1:6599"
        )]
        host: Uri,
        #[structopt(
            long,
            help = "the username for root user to be created",
            default_value = "root"
        )]
        identity: String,
        #[structopt(long, help = "whether to output the generated configuration to stdout")]
        export: bool,
    },
    #[structopt(about = "Manages locally available identities and credentials")]
    Auth {
        #[structopt(subcommand)]
        subcmd: AuthSubCommand,
    },
    #[structopt(about = "Change the state of the deployment and its attached resources")]
    Gov {
        #[structopt(
            long,
            help = "the path to the directory contained the manifest files to apply",
            default_value = "./"
        )]
        workspace_dir: PathBuf,
        #[structopt(subcommand)]
        subcmd: GovSubCommand,
    },
    #[structopt(about = "Insert and monitor jobs and retrieve their outputs")]
    Jobs {
        #[structopt(subcommand)]
        subcmd: JobSubCommand,
    },
    #[structopt(about = "Explore the Parallax virtual datasets")]
    Catalog {
        #[structopt(subcommand)]
        subcmd: CatalogSubCommand,
    },
}

#[derive(Debug, Clone, StructOpt)]
enum AuthSubCommand {
    #[structopt(about = "Lists locally owned identities")]
    List,
    #[structopt(
        about = "Exports a locally owned identity to be used again by `parallax` somewhere else"
    )]
    Export {
        #[structopt(long, help = "the known deployment for which to export identity data")]
        host: Uri,
        #[structopt(long, help = "the user name for which to export identity data")]
        identity: String,
        #[structopt(long, help = "the name of the key to export")]
        key_name: String,
    },
    #[structopt(about = "Imports an identity previously exported by `parallax`")]
    Import {
        #[structopt(long, help = "the identities to import (stdin if not specified)")]
        file: Option<PathBuf>,
    },
    #[structopt(about = "Imports a x509 certificate to trust a remote host")]
    Trust {
        #[structopt(long, help = "the host to attach the certificate to")]
        host: Uri,
        #[structopt(long, help = "the hostname to trust")]
        hostname: String,
        #[structopt(
            long,
            help = "the path to the certificate to import (if not specified, stdin)"
        )]
        cert: Option<PathBuf>,
    },
    #[structopt(
        about = "Sets the current identity. Run `parallax auth list` to see which identities are available"
    )]
    Set {
        #[structopt(long, help = "the host to set as default")]
        host: Uri,
        #[structopt(long, help = "the identity to set as default")]
        identity: String,
        #[structopt(long, help = "the key to set as default")]
        key_name: String,
    },
    #[structopt(about = "Gets the current default identity")]
    Default,
}

#[derive(Debug, Clone, StructOpt)]
enum GovSubCommand {
    #[structopt(about = "Shows the operations that would be carried on by `parallax gov apply`")]
    Plan,
    #[structopt(about = "Reconciles the remote state with local project files")]
    Apply {
        #[structopt(long, help = "whether to do a dry-run")]
        dry_run: bool,
        #[structopt(
            long,
            help = "whether to ask for confirmation before effecting any change"
        )]
        no_confirm: bool,
    },
    #[structopt(about = "Refreshes local project files to sync them up with remote")]
    Fetch {
        #[structopt(
            long,
            help = "the file to store the fetched state into (stdout if not set)"
        )]
        output: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, StructOpt)]
enum CatalogSubCommand {
    #[structopt(about = "List virtual datasets", visible_alias = "ls")]
    List {
        #[structopt(
            about = "the pattern used to identify a virtual resource. Can be empty, <dataset> or <dataset>.<table>"
        )]
        pattern: Option<String>,
    },
}

#[derive(Debug, Clone, StructOpt)]
enum JobSubCommand {
    #[structopt(
        about = "Lists jobs that have been submitted previously",
        visible_alias = "ls"
    )]
    List,
    #[structopt(about = "Inserts a new query job")]
    Insert {
        #[structopt(long, help = "the query to run (or stdin if not set)")]
        query: Option<String>,
    },
    #[structopt(about = "Gets more information about a job previously submitted")]
    Get {
        #[structopt(
            help = "the id (or any part of the id) of the job to retrieve information for"
        )]
        job_id: String,
    },
    #[structopt(about = "Downloads the output of a query job as a table")]
    Fetch {
        #[structopt(help = "the id(ish) of the job to retrieve the output of")]
        job_id: String,
        #[structopt(
            long,
            help = "save the output of the job in the specified file (if not specified, output to stdout)"
        )]
        output: Option<PathBuf>,
        #[structopt(long, help = "specify the output format", default_value = "pretty")]
        format: OutputFormat,
        #[structopt(
            long,
            help = "truncate the output to the specified number of rows (only used when `--format=pretty`)",
            default_value = "10"
        )]
        truncate: usize,
    },
    #[structopt(about = "Inserts and fetches query job while blocking. (Up to 2Gb)")]
    Query {
        #[structopt(long, help = "the query to run (or stdin if not set)")]
        query: Option<String>,
        #[structopt(
            long,
            help = "save the output of the job in the specified file (if not specified, output to stdout)"
        )]
        output: Option<PathBuf>,
        #[structopt(long, help = "specify the output format", default_value = "pretty")]
        format: OutputFormat,
        #[structopt(
            long,
            help = "truncate the output to the specified number of rows (only used when `--format=pretty`)",
            default_value = "10"
        )]
        truncate: usize,
        #[structopt(
            long,
            help = "the number of seconds to wait for the query",
            default_value = "600"
        )]
        timeout: u64,
    },
}

#[derive(Clone, Debug, StructOpt)]
enum OutputFormat {
    Csv,
    Pretty,
}

impl std::str::FromStr for OutputFormat {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "csv" => Ok(Self::Csv),
            "pretty" => Ok(Self::Pretty),
            other => Err(Error::msg(format!("unrecognised output format: {}", other))),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    let config = Config::new(opt.home.as_path())?;

    match opt.cmd {
        Command::Init {
            host,
            identity,
            export,
        } => {
            // FIXME
            let final_host = format!("{}:6548", host.host().unwrap_or("127.0.0.1"));
            let res = init(host.clone(), final_host.clone(), identity.clone()).await?;
            if export {
                let as_str = toml::ser::to_string(&res)?;
                println!("{}", as_str);
            } else {
                config.set_identities(res)?;
                config.set_default_identity(final_host, identity, "master".to_string())?;
            }
        }
        Command::Auth { subcmd } => match subcmd {
            AuthSubCommand::List => {
                let identities = config.identities()?.host;

                let default_id = config.get_default_identity().ok();

                let mut table = Table::new();
                table.add_row(row!["", "HOST", "IDENTITY", "KEY NAMES"]);

                for (uri, creds) in identities.into_iter() {
                    for (name, keyring) in creds.identity.into_iter() {
                        let is_default = if let Some(default) = default_id.as_ref() {
                            default.host == uri && default.identity == name
                        } else {
                            false
                        };

                        let prefix = if is_default { "*" } else { "" };

                        let keys: String = keyring
                            .secrets
                            .keys()
                            .fold(String::new(), |acc, v| acc + v + ", ");
                        table.add_row(row![prefix, uri, name, keys]);
                    }
                }

                table.printstd();
            }
            AuthSubCommand::Export {
                host,
                identity,
                key_name,
            } => {
                let (raw, _, _) = config.host(&host)?.identity(&identity)?.secret(&key_name)?;
                let id = Identities::new(
                    host.to_string(),
                    Identity::new(identity, Keyring::new(key_name, raw)),
                );
                let id_as_str = toml::ser::to_string(&id)?;
                println!("{}", id_as_str);
            }
            AuthSubCommand::Import { file } => {
                let mut buf = String::new();
                if let Some(path) = file {
                    File::open(path.as_path())?.read_to_string(&mut buf)?;
                } else {
                    std::io::stdin().read_to_string(&mut buf)?;
                }
                let identities = toml::de::from_str(&buf)?;
                config.set_identities(identities)?;
            }
            AuthSubCommand::Trust {
                host,
                hostname,
                cert,
            } => {
                let mut identities = config.identities()?;
                let identity = identities
                    .host
                    .get_mut(&host.to_string())
                    .ok_or(Error::msg(format!("no identity found for host {}", host)))?;

                let mut cert_str = String::new();

                if let Some(path) = cert {
                    File::open(path.as_path())?.read_to_string(&mut cert_str)?;
                } else {
                    std::io::stdin().read_to_string(&mut cert_str);
                }

                identity.cert = Some(cert_str);
                identity.hostname = Some(hostname);

                config.set_identities(identities)?;
            }
            AuthSubCommand::Set {
                host,
                identity,
                key_name,
            } => {
                config.host(&host)?.identity(&identity)?.secret(&key_name)?;
                config.set_default_identity(host.to_string(), identity, key_name)?;
            }
            AuthSubCommand::Default => {
                let identity = config.get_default_identity()?;
                let as_str = toml::ser::to_string(&identity)?;
                println!("{}", as_str);
            }
        },
        Command::Gov {
            workspace_dir,
            subcmd,
        } => match subcmd {
            GovSubCommand::Plan => {
                let mut client = config.new_client(opt.disable_tls).await?;
                let remote = gov::get_remote_scope(&mut client).await?;
                let local = gov::read_workspace_dir(workspace_dir.as_path())?;
                let plan = gov::Plan::new(remote, local)?;

                println!("{}", plan);
            }
            GovSubCommand::Apply {
                dry_run,
                no_confirm,
            } => {
                let mut client = config.new_client(opt.disable_tls).await?;
                let remote = gov::get_remote_scope(&mut client).await?;
                let local = gov::read_workspace_dir(workspace_dir.as_path())?;
                let plan = gov::Plan::new(remote, local)?;

                println!("{}", plan);

                if dry_run || plan.is_noop() {
                    return Ok(());
                }

                if !no_confirm {
                    let confirmed = Confirmation::new()
                        .with_text("Do you want to apply the plan?")
                        .interact()?;
                    if confirmed {
                        plan.execute(&mut client).await?;
                    } else {
                        println!("Aborting.");
                    }
                } else {
                    plan.execute(&mut client).await?;
                }
            }
            GovSubCommand::Fetch { output } => {
                let mut client = config.new_client(opt.disable_tls).await?;
                let remote = gov::get_remote_scope(&mut client).await?;

                let resources = remote.values().cloned().collect::<Vec<_>>();

                let as_manifest = serde_yaml::to_string(&resources)?;

                if let Some(path) = output {
                    File::create(path)?.write(as_manifest.as_bytes())?;
                } else {
                    println!("{}", as_manifest);
                }
            }
        },
        Command::Jobs { subcmd } => match subcmd {
            JobSubCommand::List => {
                let mut client = config.new_client(opt.disable_tls).await?;
                let jobs = job::list_jobs(&mut client).await?;
                job::print_jobs(jobs);
            }
            JobSubCommand::Insert { query } => {
                let query = query_or_stdin(query)?;
                let mut client = config.new_client(opt.disable_tls).await?;
                let job = job::insert_job(&mut client, &query).await?;
                println!("{}", job.id);
            }
            JobSubCommand::Get { job_id } => {
                let mut client = config.new_client(opt.disable_tls).await?;
                let job = job::find_job(&mut client, &job_id).await?;
                println!("{:#?}", job);
            }
            JobSubCommand::Fetch {
                job_id,
                output,
                format,
                truncate,
            } => {
                let mut client = config.new_client(opt.disable_tls).await?;
                let job = job::find_job(&mut client, &job_id).await?;

                if !job::is_done(&job) {
                    return Err(Error::msg("job has not finished yet"));
                }

                let fetch = job::Fetch::fetch(&mut client, &job);

                output_rows(output, format, truncate, fetch).await?
            }
            JobSubCommand::Query {
                query,
                output,
                format,
                truncate,
                timeout,
            } => {
                let query = query_or_stdin(query)?;
                let mut client = config.new_client(opt.disable_tls).await?;
                let job = Job {
                    query,
                    ..Default::default()
                };
                let fetch = job::Fetch::query(&mut client, &job, timeout);
                output_rows(output, format, truncate, fetch).await?
            }
        },
        Command::Catalog { subcmd } => match subcmd {
            CatalogSubCommand::List { pattern } => {
                let mut client = config.new_client(opt.disable_tls).await?;
                catalog::ls(&mut client, pattern).await?;
            }
        },
    }

    Ok(())
}

fn query_or_stdin(query: Option<String>) -> Result<String> {
    let query = if let Some(query) = query {
        query
    } else {
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        buf
    };
    Ok(query)
}

async fn output_rows(
    output: Option<PathBuf>,
    format: OutputFormat,
    truncate: usize,
    fetch: Fetch<'_>,
) -> Result<()> {
    match format {
        OutputFormat::Csv => {
            if let Some(path) = output {
                let file = File::create(path.as_path())?;
                fetch.write_csv(file).await?;
            } else {
                fetch.write_csv(std::io::stdout()).await?;
            }
        }
        OutputFormat::Pretty => {
            if let Some(_) = output {
                return Err(Error::msg(
                    "using `--output` not supported when `--format=pretty`",
                ));
            } else {
                let mut buf = Vec::new();

                fetch.truncate(truncate).write_csv(&mut buf).await?;

                let mut reader = csv_crate::ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(buf.as_slice());

                let mut table = Table::new();
                let mut num_cols = 0;
                for record in reader.records().take(truncate) {
                    let record = record?;
                    num_cols = record.len();
                    let row: Row = record.iter().map(|val| Cell::new(val)).collect();
                    table.add_row(row);
                }

                table.add_row((0..num_cols).into_iter().map(|_| "...").collect());

                table.printstd();
            }
        }
    }
    Ok(())
}
