//! # Parallax
//! *Parallax* is a highly configurable relational query planner and optimizer.
//! It functions by filtering inbound user queries through a set of customizable
//! policies and execution aims, prior to redirecting their modified queries to
//! third-party data warehouses/backends.
//! Please see the [github repository](https://github.com/openquery-io/parallax)
//! for the user guide and how to use it!

#![feature(
    try_blocks,
    type_ascription,
    async_closure,
    let_chains,
    associated_type_bounds,
    box_patterns
)]
#![allow(warnings)]

#[macro_use]
extern crate serde;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate clap;
use clap::Clap;

#[macro_use]
extern crate parallax_api;

#[macro_use]
extern crate entish;

use std::env;
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

pub use futures::compat::{Future01CompatExt, Stream01CompatExt};
use futures::prelude::*;

use parallax_api::{JobServiceServer, ResourceServiceServer};
use tonic::transport::{Identity, Server, ServerTlsConfig};

mod backends;
mod job;

mod node;
use crate::node::{
    access::{AccessProvider, AccountAccessProvider, BootstrapAccessProvider},
    Node,
};

mod utils;

mod opt;

#[cfg(feature = "gcp")]
mod gcp;

mod rpc;
use crate::rpc::{JobServiceImpl, ResourceServiceImpl};

mod common;
pub type Result<T> = std::result::Result<T, parallax_api::Error>;

const descriptor: &'static [u8] = include_bytes!(concat!(env!("OUT_DIR"), "/parallax.pb"));

/// A quick and hacky tide app that serves the compiled protobuf interface of the
/// query service under `/probe/descriptor` and signals being alive under `/probe/status`.
/// The first is used in the deployment topo of Parallax with Envoy as an ingress
/// gateway (which consumes it to transpile REST/JSON to gRPC).
pub async fn serve_probe(
    bind_addr: std::net::IpAddr,
    bind_port: u16,
) -> std::result::Result<(), std::io::Error> {
    let mut app = tide::new();
    app.at("/probe").nest(|r| {
        r.at("/status").get(|_| async move { "alive" });
        r.at("/descriptor")
            .get(|_| async move { tide::Response::with_reader(200, descriptor) });
    });
    let bind_addr = std::net::SocketAddr::new(bind_addr, bind_port);
    info!("probe is listening on {}", bind_addr);
    app.listen(bind_addr).await?;
    Ok(())
}

pub async fn serve_rpc<A: AccessProvider + 'static>(
    access: A,
    bind_host: std::net::IpAddr,
    bind_port: u16,
    enable_tls: Option<Identity>,
) -> std::result::Result<(), tonic::transport::Error> {
    let bound = format!("{}:{}", bind_host, bind_port).parse().unwrap();
    info!("an rpc server is listening on {}", bound);
    let mut builder = Server::builder();

    if let Some(identity) = enable_tls {
        let tls_config = ServerTlsConfig::new().identity(identity);
        builder = builder.tls_config(tls_config);
    }

    builder
        .add_service(JobServiceServer::new(JobServiceImpl::new(access.clone())))
        .add_service(ResourceServiceServer::new(ResourceServiceImpl::new(
            access.clone(),
        )))
        .serve(bound)
        .await
        .map_err(|e| e.into())
}

#[derive(Clap)]
#[clap(name = "parallax-worker")]
pub struct Opt {
    #[clap(long = "redis-host", default_value = "127.0.0.1", env = "REDIS_HOST")]
    pub redis_host: String,
    #[clap(long = "redis-port", default_value = "6379", env = "REDIS_PORT")]
    pub redis_port: u16,
    #[clap(long = "redis-pass", env = "REDIS_PASS")]
    pub redis_password_file: Option<PathBuf>,
    #[clap(long = "advertised-host", env = "ADVERTISED_HOST")]
    pub advertised_host: Option<std::net::IpAddr>,
    #[clap(long = "probe-host", default_value = "0.0.0.0", env = "PROBE_HOST")]
    pub probe_host: std::net::IpAddr,
    #[clap(long = "probe-port", default_value = "7331", env = "PROBE_PORT")]
    pub probe_port: u16,
    #[clap(long = "cluster-host", default_value = "0.0.0.0", env = "CLUSTER_HOST")]
    pub cluster_host: std::net::IpAddr,
    #[clap(long = "cluster-port", default_value = "7865", env = "CLUSTER_PORT")]
    pub cluster_port: u16,
    #[clap(long = "rpc-host", default_value = "0.0.0.0", env = "RPC_HOST")]
    pub rpc_host: std::net::IpAddr,
    #[clap(long = "rpc-port", default_value = "6548", env = "RPC_PORT")]
    pub rpc_port: u16,
    #[clap(long = "disable-tls")]
    pub disable_tls: bool,
    #[clap(long = "enable-bootstrap")]
    pub enable_bootstrap: bool,
    #[clap(long = "identity")]
    pub identity: Option<PathBuf>,
    #[clap(long = "secret")]
    pub secret: Option<PathBuf>,
    #[clap(long = "bootstrap-rpc-host", default_value = "127.0.0.1")]
    pub bootstrap_rpc_host: std::net::IpAddr,
    #[clap(long = "bootstrap-rpc-port", default_value = "6599")]
    pub bootstrap_rpc_port: u16,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse();

    tokio::spawn(serve_probe(opt.probe_host, opt.probe_port));

    let node = Arc::new(node::new(&opt).await?);

    if opt.enable_bootstrap {
        let b_host = opt.bootstrap_rpc_host;
        let b_port = opt.bootstrap_rpc_port;
        info!(
            "--enable-bootstrap is set; spawning bootstrapping client on {}:{}",
            b_host, b_port
        );
        let bootstrap_access = BootstrapAccessProvider::new(node.clone());
        tokio::spawn(serve_rpc(bootstrap_access, b_host, b_port, None));
    }

    let account_access = AccountAccessProvider::new(node);
    let c_host = opt.rpc_host;
    let c_port = opt.rpc_port;

    let tls_identity = if opt.disable_tls {
        warn!("disabling TLS in public facing RPC endpoint: NOT RECOMMENDED");
        None
    } else {
        let mut identity = Vec::new();
        File::open(
            &opt.identity
                .expect("--identity required when tls is enabled"),
        )?
        .read_to_end(&mut identity)?;

        let mut secret = Vec::new();
        File::open(&opt.secret.expect("--secret required when tls is enabled"))?
            .read_to_end(&mut secret)?;

        let identity_with_secret = Identity::from_pem(&identity, &secret);
        Some(identity_with_secret)
    };

    tokio::spawn(serve_rpc(account_access, c_host, c_port, tls_identity));

    tokio_signal::ctrl_c()
        .compat()
        .map_ok(|s| s.compat())
        .try_flatten_stream()
        .into_future()
        .await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn setup_test() {
        env_logger::builder().is_test(true).init();
    }

    #[test]
    fn probe() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();

        let serve_probe_fut = serve_probe("0.0.0.0".parse().unwrap(), 7331);
        rt.spawn(serve_probe_fut);

        rt.block_on(async move {
            let is_alive = reqwest::get("http://127.0.0.1:7331/probe/status")
                .await
                .unwrap()
                .text()
                .await
                .unwrap();

            assert_eq!("alive".to_string(), is_alive);

            let has_descriptor = reqwest::get("http://127.0.0.1:7331/probe/descriptor")
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();

            assert_eq!(descriptor, has_descriptor);
        })
    }
}
