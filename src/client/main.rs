use std::io::Write;
use std::path::PathBuf;

use clap::Parser;

use velociraptor_api::{Client, ClientConfig, QueryOptions};

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
    Fetch(FetchCmd),
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
    query: Vec<String>,
}

#[derive(clap::Args, Clone, Debug)]
struct FetchCmd {
    #[clap(long)]
    output_file: PathBuf,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let client = Client::try_from(&ClientConfig::from_yaml_file(cli.config.clone())?)?;

    match cli.sub {
        SubCommand::Query(ref cmd) => {
            let result = client
                .query(
                    cmd.query.as_slice(),
                    &QueryOptions::new()
                        .env(cmd.env.as_ref())
                        .org_id(cmd.org.clone())
                        .build(),
                )
                .await?;
            println!("Result:\n{}\n", serde_json::to_string_pretty(&result)?);
        }
        SubCommand::Fetch(ref cmd) => {
            let buf = client.fetch(&cmd.path).await?;

            let mut output = std::fs::File::create(&cmd.output_file)?;
            output.write_all(&buf)?;
            output.flush()?;
        }
    }

    Ok(())
}
