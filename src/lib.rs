use std::path::{Component, Path};

use serde::{de::DeserializeOwned, Deserialize};

use tokio::time::{sleep, Duration};
use tokio_stream::StreamExt;

use tonic::{
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Uri},
    IntoRequest,
};

use typed_builder::TypedBuilder;

use thiserror::Error;

use proto::*;
mod proto;

/// Client configuration for the Velociraptor gRPC API
#[allow(dead_code)]
#[derive(Deserialize)]
pub struct APIClientConfig {
    ca_certificate: String,
    client_cert: String,
    client_private_key: String,
    api_connection_string: String,
    name: String,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to read file: {0}")]
    IO(std::io::Error),
    #[error("Failed to parse YAML: {0}")]
    YAML(serde_yaml::Error),
}

#[derive(Debug, Error)]
pub enum APIClientError {
    #[error("Failed to build API client: {0}")]
    HTTP(tonic::codegen::http::Error),
    #[error("Transport error: {0}")]
    Transport(tonic::transport::Error),
    #[error("Failed to run RPC: {0}")]
    Status(tonic::Status),
    #[error("Failed to parse response: {0}")]
    MalformedResponse(serde_json::Error),
    #[error("Failed to run VQL query: {0}")]
    VQL(String),
}

impl APIClientConfig {
    /// Construct client configuration from YAML file as generated by
    /// `velociraptor config api_client --name $NAME $OUT_FILE`
    pub fn from_yaml_file<P: AsRef<Path>>(path: &P) -> Result<Self, ConfigError> {
        let cc = serde_yaml::from_reader(std::fs::File::open(path).map_err(ConfigError::IO)?)
            .map_err(ConfigError::YAML)?;
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

/// APIClient for the Velociraptor gRPC API
pub struct APIClient {
    endpoint: Endpoint,
}

impl TryFrom<&APIClientConfig> for APIClient {
    type Error = APIClientError;
    fn try_from(cfg: &APIClientConfig) -> Result<Self, Self::Error> {
        let uri = Uri::builder()
            .scheme("https")
            .authority(cfg.api_connection_string.as_str())
            .path_and_query("/")
            .build()
            .map_err(APIClientError::HTTP)?;
        let endpoint = Endpoint::from(uri)
            .tls_config(cfg.tls_config())
            .map_err(APIClientError::Transport)?;
        Ok(Self { endpoint })
    }
}

/// Options for the `query` method
#[derive(TypedBuilder)]
pub struct QueryOptions {
    /// Envirnment variables to be passed along with the query
    #[builder(default, setter(into))]
    env: Vec<(String, String)>,
    /// Organization ID
    #[builder(default, setter(into))]
    org_id: Option<String>,
    /// Maximum rows to return at a time
    #[builder(default)]
    max_row: u64,
}

impl APIClient {
    async fn api_client(&self) -> Result<api_client::ApiClient<Channel>, tonic::transport::Error> {
        Ok(api_client::ApiClient::new(self.endpoint.connect().await?))
    }

    /// Issue a server-side VQL query
    pub async fn sync_query<T: DeserializeOwned>(
        &self,
        query: &str,
        options: &QueryOptions,
    ) -> Result<Vec<T>, APIClientError> {
        let env = options
            .env
            .iter()
            .cloned()
            .map(|(key, value)| VqlEnv { key, value })
            .collect::<Vec<_>>();
        let org_id = options.org_id.clone().unwrap_or_default();
        let query = vec![VqlRequest {
            name: "".into(),
            vql: query.into(),
        }];
        let max_row = options.max_row;

        let mut response = self
            .api_client()
            .await
            .map_err(APIClientError::Transport)?
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
            .await
            .map_err(APIClientError::Status)?
            .into_inner();

        let mut result = vec![];
        while let Some(Ok(msg)) = response.next().await {
            if !msg.response.is_empty() {
                log::trace!("result = {}", &msg.response);
                result.append(
                    &mut serde_json::from_str(&msg.response)
                        .map_err(APIClientError::MalformedResponse)?,
                );
            }
            if !msg.log.is_empty() {
                log::debug!("log = {}", msg.log.trim());
                if msg.log.starts_with("VQL Error:") {
                    return Err(APIClientError::VQL(msg.log));
                }
            }
        }

        Ok(result)
    }

    /// Fetch downloadable file from Velociraptor server
    pub async fn fetch<P: AsRef<Path>>(&self, path: P) -> Result<Vec<u8>, APIClientError> {
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

        let mut api_client = self.api_client().await.map_err(APIClientError::Transport)?;
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
                .await
                .map_err(APIClientError::Status)?
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

    pub fn new_client_unchecked(&self, id: &str) -> Client {
        Client {
            api_client: self,
            client_id: id.to_string(),
        }
    }
}

/// Representation of a Velociraptor client
pub struct Client<'a> {
    api_client: &'a APIClient,
    client_id: String,
}

impl std::fmt::Display for Client<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.client_id)
    }
}

impl Client<'_> {
    pub async fn schedule_flow(
        &self,
        artifact: &str,
        cmd: &str,
    ) -> Result<ClientFlow, APIClientError> {
        #[derive(Deserialize)]
        struct Request {
            flow_id: String,
        }
        #[derive(Deserialize)]
        struct Submit {
            request: Request,
        }

        let env = vec![
            ("client_id".to_string(), self.client_id.to_string()),
            ("artifact".to_string(), artifact.to_string()),
            ("Command".to_string(), cmd.to_string()),
        ];
        let requests: Vec<Submit> = self
            .api_client
            .sync_query(
                r#"SELECT
                   collect_client(client_id=client_id,
                                  artifacts=artifact,
                                  env=dict(Command=Command))
                   AS request
                   FROM scope()"#,
                &QueryOptions::builder()
                    .env(env.as_slice())
                    .org_id("".to_string())
                    .build(),
            )
            .await?;

        Ok(ClientFlow {
            api_client: self.api_client,
            client_id: self.client_id.clone(),
            flow_id: requests[0].request.flow_id.clone(),
        })
    }
}

/// Representation of a flow scheduled to executed by a Velociraptor client.
pub struct ClientFlow<'a> {
    api_client: &'a APIClient,
    client_id: String,
    flow_id: String,
}

impl std::fmt::Display for ClientFlow<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.flow_id)
    }
}

impl ClientFlow<'_> {
    pub async fn fetch<T: DeserializeOwned>(&self) -> Result<Vec<T>, APIClientError> {
        #[derive(Clone, Default, Deserialize)]
        struct FlowStatus {
            state: String, // UNSET, RUNNING, FINISHED, ERROR
        }

        let options = QueryOptions::builder()
            .env(vec![
                ("client_id".into(), self.client_id.clone()),
                ("flow_id".into(), self.flow_id.clone()),
            ])
            .org_id("".to_string())
            .build();

        loop {
            log::debug!("Looking for {} / {} ...", self.client_id, self.flow_id);
            let status = self
                .api_client
                .sync_query::<FlowStatus>(
                    r#"SELECT * FROM flows(client_id=client_id, flow_id=flow_id)"#,
                    &options,
                )
                .await?;
            let state = status.first().cloned().unwrap_or_default().state;
            log::debug!("state( {} / {} ): {state}", &self.client_id, &self.flow_id);
            if state != "RUNNING" {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        log::debug!(
            "Fetching result for {} / {} ...",
            self.client_id,
            self.flow_id
        );
        loop {
            let result = self
                .api_client
                .sync_query::<T>(
                    r#"SELECT * FROM flow_results(client_id=client_id, flow_id=flow_id)"#,
                    &options,
                )
                .await?;
            if !result.is_empty() {
                log::debug!("Done!");
                return Ok(result);
            }
            log::trace!("zZz...");
            sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn fetch_log(&self) -> Result<Vec<FlowLogEntry>, APIClientError> {
        let options = QueryOptions::builder()
            .env(vec![
                ("client_id".into(), self.client_id.clone()),
                ("flow_id".into(), self.flow_id.clone()),
            ])
            .org_id("".to_string())
            .build();
        let mut result: Vec<FlowLogEntry>;
        loop {
            result = self
                .api_client
                .sync_query(
                    r#"SELECT * FROM flow_logs(client_id=client_id, flow_id=flow_id)"#,
                    &options,
                )
                .await?;
            if result.is_empty() {
                sleep(Duration::from_millis(100)).await;
                log::debug!("Retrying...");
            } else {
                for r in &result {
                    log::debug!(
                        "flow_log({}/{}): {} {}: {}",
                        self.client_id,
                        self.flow_id,
                        r.client_time,
                        r.level,
                        r.message
                    );
                }
                return Ok(result);
            }
        }
    }
}

/// A single flow log entry
#[derive(Deserialize)]
pub struct FlowLogEntry {
    pub client_time: u64,
    pub level: String,
    pub message: String,
}
