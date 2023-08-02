use std::path::PathBuf;

use futures::stream::StreamExt;

use clap::Parser;

use velociraptor_api::{ApiClient, VqlCollectorArgs, VqlEnv, VqlRequest};

#[derive(Parser, Debug, Clone)]
#[clap(version, about)]
struct Cli {
    #[clap(long)]
    /// Path to the API client config. You can generate such a file with "velociraptor config api_client"
    config: PathBuf,
    #[clap(subcommand)]
    sub: SubCommand,
}

#[derive(Clone, Debug, Parser)]
enum SubCommand {
    /// Issue a server side VQL query
    Query(QueryCmd),
}

#[derive(clap::Args, Clone, Debug)]
struct QueryCmd {
    /// Org ID to use
    #[clap(long)]
    org: Option<String>,
    /// Add query environment values in the form of Key=Value
    #[clap(long,value_parser=parse_key_val::<String,String>)]
    env: Vec<(String, String)>,
    /// The query to run
    #[clap(value_parser)]
    query: String,
}

#[derive(clap::Args, Clone, Debug)]
struct FetchCmd {
    #[clap(value_parser)]
    client: String,
    #[clap(value_parser)]
    path: PathBuf,
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(
    s: &str,
) -> Result<(T, U), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: std::error::Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

use tonic::transport::Channel;

async fn do_query(
    client: &mut ApiClient<Channel>,
    cmd: &QueryCmd,
) -> Result<(), Box<dyn std::error::Error>> {
    let org_id = cmd.org.clone().unwrap_or_else(|| "root".into());
    let env = cmd
        .env
        .iter()
        .cloned()
        .map(|(key, value)| VqlEnv { key, value })
        .collect::<Vec<_>>();

    let request = tonic::Request::new(VqlCollectorArgs {
        env,
        org_id,
        max_row: 10,
        query: vec![VqlRequest {
            name: "Test?!".into(),
            vql: cmd.query.to_string(),
        }],
        ..VqlCollectorArgs::default()
    });

    let mut response = client.query(request).await?.into_inner();

    let mut result: Vec<serde_json::Value> = vec![];
    while let Some(Ok(msg)) = response.next().await {
        if !msg.response.is_empty() {
            let mut rows: Vec<serde_json::Value> = serde_json::from_str(&msg.response)?;
            result.append(&mut rows);
        }
        if !msg.log.is_empty() {
            print!("Log: {}", msg.log);
        }
    }

    println!("Result:\n\n{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let channel = velociraptor_api::Config::from_yaml_file(cli.config.clone())?
        .endpoint()?
        .connect()
        .await?;

    let mut client = ApiClient::new(channel);
    match cli.sub {
        SubCommand::Query(ref cmd) => {
            do_query(&mut client, cmd).await?;
        }
    }

    Ok(())
}
