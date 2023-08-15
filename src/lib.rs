use std::collections::BTreeMap;
use std::path::{Component, Path};

use tokio_stream::StreamExt;

use builder_pattern::Builder;
use serde::{de::DeserializeOwned, Deserialize};
use tonic::{
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Uri},
    IntoRequest,
};

use proto::*;
mod proto;

use api_client::ApiClient;

/// Client configuration for the Velociraptor gRPC API
#[allow(dead_code)]
#[derive(Deserialize)]
pub struct ClientConfig {
    ca_certificate: String,
    client_cert: String,
    client_private_key: String,
    api_connection_string: String,
    name: String,
}

impl ClientConfig {
    /// Construct client configuration from YAML file as generated by
    /// `velociraptor config api_client --name $NAME $OUT_FILE`
    pub fn from_yaml_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let cc = serde_yaml::from_reader(std::fs::File::open(path)?)?;
        Ok(cc)
    }

    fn tls_config(&self) -> ClientTlsConfig {
        let ca = Certificate::from_pem(self.ca_certificate.clone());
        let id = Identity::from_pem(self.client_cert.clone(), self.client_private_key.clone());
        ClientTlsConfig::new()
            .domain_name("VelociraptorServer")
            .ca_certificate(ca)
            .identity(id)
    }
}

/// Client for the Velociraptor gRPC API
pub struct Client {
    endpoint: Endpoint,
}

impl TryFrom<&ClientConfig> for Client {
    type Error = Box<dyn std::error::Error>;
    fn try_from(cfg: &ClientConfig) -> Result<Self, Self::Error> {
        let uri = Uri::builder()
            .scheme("https")
            .authority(cfg.api_connection_string.as_str())
            .path_and_query("/")
            .build()?;
        let endpoint = Endpoint::from(uri).tls_config(cfg.tls_config())?;
        Ok(Self { endpoint })
    }
}

/// Options for the `query` method
#[derive(Builder)]
pub struct QueryOptions {
    #[public]
    #[into]
    /// Envirnment variables to be passed along with the query
    env: Vec<(String, String)>,
    #[public]
    #[into]
    /// Organization ID
    org_id: Option<String>,
    /// Maximum rows to return at a time
    #[public]
    #[default(10)]
    max_row: u64,
}

pub struct QueryList(Vec<(String, String)>);

impl From<&str> for QueryList {
    fn from(item: &str) -> Self {
        Self(vec![("query".to_string(), item.to_string())])
    }
}

impl From<String> for QueryList {
    fn from(item: String) -> Self {
        Self::from(item.as_str())
    }
}

impl From<&[(String, String)]> for QueryList {
    fn from(items: &[(String, String)]) -> Self {
        Self(items.to_vec())
    }
}

impl From<&[String]> for QueryList {
    fn from(items: &[String]) -> Self {
        Self(
            items
                .iter()
                .cloned()
                .enumerate()
                .map(|(n, s)| (format!("query-{}", n), s))
                .collect(),
        )
    }
}

impl<'a> Client {
    async fn api_client(&self) -> Result<ApiClient<Channel>, Box<dyn std::error::Error>> {
        Ok(ApiClient::new(self.endpoint.connect().await?))
    }

    /// Issue a server-side VQL query
    pub async fn query<T: DeserializeOwned>(
        &self,
        queries: impl Into<QueryList>,
        options: &QueryOptions,
    ) -> Result<BTreeMap<String, Vec<T>>, Box<dyn std::error::Error>> {
        let env = options
            .env
            .iter()
            .cloned()
            .map(|(key, value)| VqlEnv { key, value })
            .collect::<Vec<_>>();
        let org_id = options.org_id.clone().unwrap_or_default().clone();
        let query = queries
            .into()
            .0
            .into_iter()
            .map(|(name, vql)| VqlRequest { name, vql })
            .collect();
        let max_row = options.max_row;

        let mut response = self
            .api_client()
            .await?
            .query(
                VqlCollectorArgs {
                    env,
                    org_id,
                    max_row,
                    query,
                    ..VqlCollectorArgs::default()
                }
                .into_request(),
            )
            .await?
            .into_inner();

        let mut result: BTreeMap<String, Vec<T>> = BTreeMap::new();
        while let Some(Ok(msg)) = response.next().await {
            if !msg.response.is_empty() {
                let mut rows: Vec<T> = serde_json::from_str(&msg.response)?;
                result
                    .entry(msg.query.unwrap().name)
                    .and_modify(|r| r.append(&mut rows))
                    .or_insert(rows);
            }
            if !msg.log.is_empty() {
                // log::trace!("{}", msg.log);
            }
        }

        Ok(result)
    }

    /// Fetch downloadable file from Velociraptor server
    pub async fn fetch<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let components: Vec<_> = path
            .as_ref()
            .components()
            .filter_map(|c| match c {
                Component::Normal(s) => Some(s.to_string_lossy().to_string()),
                _ => None,
            })
            .collect();

        let request = VfsFileBuffer {
            components,
            length: 1024,
            ..VfsFileBuffer::default()
        };

        let mut api_client = self.api_client().await?;
        let (mut buf, mut offset) = (vec![], 0);
        loop {
            let response = api_client
                .vfs_get_buffer(
                    VfsFileBuffer {
                        offset,
                        ..request.clone()
                    }
                    .into_request(),
                )
                .await?
                .into_inner();

            match response.data.len() {
                0 => break,
                len => {
                    buf.extend(response.data);
                    offset += len as u64;
                }
            };
        }
        Ok(buf)
    }
}
