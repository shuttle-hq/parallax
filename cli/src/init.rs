use colored::Colorize;

use std::sync::Arc;

use anyhow::{Error, Result};

use tokio_rustls::rustls::{Certificate, ClientConfig};
use tonic::transport::Uri;

use parallax_api::{
    client::{extract_public_key_pem, generate_rsa_key_pair_pem, Client, ClientTlsConfig},
    Block, CreateResourceRequest, Group, ListResourcesRequest, Resource, User,
};

use crate::config::{Identities, Identity, Keyring};

pub async fn init(host: Uri, final_host: String, identity: String) -> Result<Identities> {
    let host_part = host.host().ok_or(Error::msg("no host specified"))?;
    let port_part = host.port_u16().ok_or(Error::msg("no port specified"))?;

    let mut client = Client::builder()
        .host(host_part)
        .port(port_part)
        .disable_tls()
        .build()
        .await?;

    let find_wheel_req = ListResourcesRequest {
        pattern: "resource.group.wheel".to_string(),
    };
    let list_resp = client
        .list_resources(tonic::Request::new(find_wheel_req))
        .await?
        .into_inner();
    if !list_resp.resources.is_empty() {
        return Err(Error::msg("already initialised"));
    }

    let private_key = generate_rsa_key_pair_pem()?;
    let public_key = extract_public_key_pem(private_key.as_bytes())?;

    let user = User {
        name: identity.clone(),
        public_keys: vec![public_key],
        primary_group: "resource.group.wheel".to_string(),
        super_user: true,
        ..Default::default()
    };

    let user_resource = Resource {
        resource: Some(user.into()),
    };

    let as_member = user_resource.block_type().unwrap().to_string();

    let wheel_group = Group {
        name: "wheel".to_string(),
        members: vec![as_member],
        ..Default::default()
    };

    let group_resource: Resource = Resource {
        resource: Some(wheel_group.into()),
    };

    let create_user_req = tonic::Request::new(CreateResourceRequest {
        resource: Some(user_resource),
    });
    client.create_resource(create_user_req).await?;

    let create_group_req = tonic::Request::new(CreateResourceRequest {
        resource: Some(group_resource),
    });
    client.create_resource(create_group_req).await?;

    let identity = Identities::new(
        final_host,
        Identity::new(identity, Keyring::new("master".to_string(), private_key)),
    );

    println!(
        "{}",
        "\nYour Parallax deployment has been successfully initialised!\n"
            .bold()
            .green()
    );
    println!(
        "{}",
        "Visit https://openquery.io/parallax/docs to get started connecting your databases."
            .green()
    );

    Ok(identity)
}
