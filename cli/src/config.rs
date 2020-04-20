use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::fs::{remove_file, DirBuilder, File};
use std::io::{Read, Write};
use std::ops::Drop;
use std::path::{Path, PathBuf};

use tonic::transport::{Certificate, Uri};

use ring::signature::RsaKeyPair;

use anyhow::{Context, Error, Result};

use parallax_api::client::{rsa_key_pem_to_der, Authenticator, Client, ClientTlsConfig};

use crate::Opt;

use crate::job::Jobs;

static DEFAULT_HOME: &'static str = "~/.config/parallax/";

#[derive(Serialize, Deserialize)]
pub struct Keyring {
    pub secrets: HashMap<String, String>,
}

impl Keyring {
    pub fn secret(&self, key_name: &str) -> Result<(String, Vec<u8>, RsaKeyPair)> {
        let secret = self
            .secrets
            .get(key_name)
            .ok_or(Error::msg(format!("no secret named {}", key_name)))?;
        let as_der = rsa_key_pem_to_der(&secret)?;
        let key_pair =
            RsaKeyPair::from_der(&as_der).map_err(|e| Error::msg(format!("invalid key: {}", e)))?;
        Ok((secret.clone(), as_der, key_pair))
    }

    pub fn new(name: String, key: String) -> Self {
        Self {
            secrets: vec![(name, key)].into_iter().collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Identity {
    pub cert: Option<String>,
    pub hostname: Option<String>,
    pub identity: HashMap<String, Keyring>,
}

impl Identity {
    pub fn new(identity: String, keyring: Keyring) -> Self {
        Self {
            cert: None,
            hostname: None,
            identity: vec![(identity, keyring)].into_iter().collect(),
        }
    }
}

impl Identity {
    pub fn identity(&self, name: &str) -> Result<&Keyring> {
        self.identity.get(name).ok_or(Error::msg(format!(
            "no user {} in identity file for this host",
            name
        )))
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct Identities {
    pub host: HashMap<String, Identity>,
}

impl Identities {
    pub fn new(host: String, identity: Identity) -> Self {
        Identities {
            host: vec![(host, identity)].into_iter().collect(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct DefaultIdentity {
    pub host: String,
    pub identity: String,
    pub key_name: String,
}

pub struct Config {
    home: PathBuf,
}

impl Config {
    pub fn new(home: &Path) -> Result<Self> {
        DirBuilder::new()
            .recursive(true)
            .create(home)
            .context(format!(
                "could not create parallax home ({})",
                home.display()
            ))?;

        let lock_path = Self::lock_path(home);

        match File::open(lock_path.as_path()) {
            Ok(_) => {
                let err_msg = format!(
                    "{} already exists: another parallax operation is in progress",
                    lock_path.to_string_lossy()
                );
                Err(Error::msg(err_msg))
            }
            Err(_) => {
                File::create(lock_path.as_path())?;
                Ok(Config {
                    home: home.to_owned(),
                })
            }
        }
    }

    pub fn get_jobs(&self) -> Result<Jobs> {
        let jobs_path = self.get_path("jobs.yaml");
        match File::open(jobs_path.as_path()) {
            Ok(mut file) => {
                let mut buf = String::new();
                file.read_to_string(&mut buf)?;
                let jobs = serde_yaml::from_str(&buf)?;
                Ok(jobs)
            }
            Err(_) => {
                let jobs = Jobs::default();
                self.set_jobs(jobs.clone())?;
                Ok(jobs)
            }
        }
    }

    pub fn set_jobs(&self, jobs: Jobs) -> Result<()> {
        let jobs_path = self.get_path("jobs.yaml");
        let as_str = serde_yaml::to_string(&jobs)?;
        File::create(jobs_path.as_path())?.write(as_str.as_bytes())?;
        Ok(())
    }

    pub fn set_default_identity(
        &self,
        host: String,
        identity: String,
        key_name: String,
    ) -> Result<()> {
        let id = DefaultIdentity {
            host,
            identity,
            key_name,
        };
        let id_as_str = toml::ser::to_string(&id)?;
        File::create(self.get_path("default.toml").as_path())?.write(id_as_str.as_bytes())?;
        Ok(())
    }

    pub fn get_default_identity(&self) -> Result<DefaultIdentity> {
        let mut buf = String::new();
        File::open(self.get_path("default.toml").as_path())
            .context("could not find default identity (set it with `parallax auth set`)")?
            .read_to_string(&mut buf)?;
        let id = toml::de::from_str(&buf)?;
        Ok(id)
    }

    pub async fn new_client(&self, disable_tls: bool) -> Result<Client> {
        let DefaultIdentity {
            host,
            identity,
            key_name,
        } = self.get_default_identity()?;

        let uri = host.parse()?;

        let config_for_host = self.host(&uri)?;

        let (_, as_der, _) = config_for_host.identity(&identity)?.secret(&key_name)?;

        let host_name = uri.host().ok_or(Error::msg("no hostname in host uri"))?;

        let host_port = uri.port_u16().unwrap_or(6548);

        let auth = Authenticator::builder()
            .user_name(&identity)
            .secret_key(&as_der)
            .build()?;

        let mut builder = Client::builder()
            .host(&host_name)
            .port(host_port)
            .authenticator(auth);

        if disable_tls {
            builder = builder.disable_tls();
        } else {
            if let Some(cert) = config_for_host.cert {
                let certificate = Certificate::from_pem(&cert);

                let mut client_tls_config = ClientTlsConfig::new().ca_certificate(certificate);

                if let Some(hostname) = config_for_host.hostname {
                    client_tls_config = client_tls_config.domain_name(hostname);
                }

                builder = builder.client_tls_config(client_tls_config);
            }
        }

        let client = builder.build().await?;

        Ok(client)
    }

    fn lock_path(home: &Path) -> PathBuf {
        home.join(Path::new(".lock"))
    }

    fn get_path<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        self.home.join(path.as_ref())
    }

    pub fn set_identities(&self, new: Identities) -> Result<()> {
        let new_str = toml::ser::to_string(&new)?;
        // FIXME: this replaces the whole thing for now
        File::create(self.get_path("identities.toml"))?.write(new_str.as_bytes())?;
        Ok(())
    }

    pub fn identities(&self) -> Result<Identities> {
        let mut buf = Vec::new();
        let identities_path = self.get_path("identities.toml");

        let identities = if let Ok(mut file) = File::open(identities_path.as_path()) {
            file.read_to_end(&mut buf)?;
            toml::de::from_slice::<Identities>(&buf)?
        } else {
            let mut file = File::create(identities_path.as_path())?;
            let identities = Identities::default();
            let buf = toml::to_string(&identities)?;
            file.write(buf.as_bytes())?;
            identities
        };

        Ok(identities)
    }

    pub fn host(&self, uri: &Uri) -> Result<Identity> {
        self.identities()?
            .host
            .into_iter()
            .map(|(uri, id)| {
                uri.parse::<Uri>()
                    .map(|uri| (uri, id))
                    .context(format!("invalid uri {}", uri))
            })
            .collect::<Result<HashMap<Uri, Identity>>>()?
            .remove(&uri)
            .ok_or(Error::msg(format!("no host {} found in identities", uri)))
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        let lock_path = Self::lock_path(&self.home);
        remove_file(lock_path.as_path()).expect("could not remove configuration lock")
    }
}
