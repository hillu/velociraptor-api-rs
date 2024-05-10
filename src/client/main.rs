use std::io::Write;
use std::path::PathBuf;

use clap::Parser;

use velociraptor_api::{APIClient, APIClientConfig, QueryOptions};

use serde::{Deserialize, Serialize};

fn config_yml_file(i: Option<String>) -> PathBuf {
    let mut f = dirs::config_dir().unwrap();
    f.push("velociraptor");
    if let Some(i) = i {
        f.push(format!("apiclient-{i}.yaml"));
    } else {
        f.push("apiclient.yaml");
    }
    f
}

#[derive(Parser, Debug, Clone)]
#[clap(version, about)]
struct Cli {
    #[clap(long)]
    /// Path to the API client config. You can generate such a file with "velociraptor config api_client"
    config: Option<PathBuf>,
    #[clap(long)]
    instance: Option<String>,
    #[clap(subcommand)]
    sub: SubCommand,
}

#[derive(Clone, Debug, Parser)]
enum SubCommand {
    /// Issue a server side VQL query
    Query(QueryCmd),
    /// Execute command or VQL query on a client
    Client(ClientCmd),
    /// Fetch a file from server
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
    query: String,
}

#[derive(Parser, Clone, Debug)]
struct ClientCmd {
    /// Client ID
    #[clap(value_parser)]
    client: String,
    #[clap(subcommand)]
    sub: ClientSubCommand,
}

#[derive(clap::Subcommand, Clone, Debug)]
enum ClientSubCommand {
    /// Issue a client side VQL query
    Query(ClientQueryCmd),
    /// Issue a client shell command
    Bash(CmdArgs),
    /// Issue a client command using CMD.EXE
    Cmd(CmdArgs),
    /// Issue a client command using PowerShell
    Powershell(CmdArgs),
}

#[derive(Clone, Debug, Parser)]
struct CmdArgs {
    #[clap(value_parser)]
    command: String,
}

#[derive(clap::Args, Clone, Debug)]
struct ClientQueryCmd {
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
    #[clap(long)]
    /// Name of (local) output file
    output_file: PathBuf,
    #[clap(value_parser)]
    /// Name of (remote) file, usually in the form of
    /// downloads/C.XXXXXXXXXXXXXXXX/F.YYYYYYYYYYYYY/HOSTNAME-C.XXXXXXXXXXXXXXXX-F.YYYYYYYYYYYYY.zip
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

#[derive(Debug, Serialize, Deserialize, Default)]
struct ShellResult {
    #[serde(rename = "Stdout")]
    stdout: String,
    #[serde(rename = "Stderr")]
    stderr: String,
    #[serde(rename = "ReturnCode")]
    returncode: i32,
    #[serde(rename = "Complete")]
    finished: bool,
}

impl ShellResult {
    fn do_output(&self) -> Result<(), Box<dyn std::error::Error>> {
        write!(std::io::stdout(), "{}", self.stdout)?;
        write!(std::io::stderr(), "{}", self.stderr)?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    env_logger::init();

    let client_yaml: PathBuf = match (cli.config, cli.instance) {
        (Some(c), None) => c,
        (None, x) => config_yml_file(x),
        _ => return Err("can't use config and instance simultaneously".into()),
    };

    let api_client = APIClient::try_from(
        &APIClientConfig::from_yaml_file(&client_yaml)
            .map_err(|e| format!("read config: {} {e}", client_yaml.to_string_lossy()))?,
    )?;

    match cli.sub {
        SubCommand::Query(ref cmd) => {
            let result = api_client
                .sync_query::<serde_json::Value>(
                    &cmd.query,
                    &QueryOptions::builder()
                        .env(cmd.env.clone())
                        .org_id(cmd.org.clone())
                        .build(),
                )
                .await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        SubCommand::Client(ClientCmd {
            client: client_id,
            sub: ClientSubCommand::Query(ref cmd),
        }) => {
            let client = api_client.new_client_unchecked(&client_id);
            let flow = client
                .schedule_flow("Generic.Client.VQL", &cmd.query)
                .await?;
            log::debug!("Flow ID: {flow}");
            // FIXME: Use select?
            // FIXME: Use SELECT state FROM flows()?
            let log = flow.fetch_log().await?;
            let mut err = false;
            for entry in log {
                let timestamp =
                    time::OffsetDateTime::from_unix_timestamp(entry.client_time as _).unwrap();
                log::debug!("log: {timestamp} {}: {}", entry.level, entry.message);
                if entry.level == "ERROR" || entry.level == "WARN" {
                    writeln!(
                        std::io::stderr(),
                        "{timestamp} {}: {}",
                        entry.level,
                        entry.message
                    )?;
                }
                if entry.level == "ERROR" {
                    err = true;
                }
            }
            if err {
                return Err(format!("Flow {flow} failed.").into());
            }
            let result: Vec<serde_json::Value> = flow.fetch().await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        SubCommand::Client(ClientCmd {
            client: client_id,
            sub: ClientSubCommand::Cmd(ref cmd),
        }) => {
            let client = api_client.new_client_unchecked(&client_id);
            let flow = client
                .schedule_flow("Windows.System.CmdShell", &cmd.command)
                .await?;
            log::debug!("Flow ID: {flow}");
            flow.fetch()
                .await?
                .into_iter()
                .fold::<ShellResult, _>(ShellResult::default(), |acc, item: ShellResult| {
                    ShellResult {
                        stdout: acc.stdout + &item.stdout,
                        stderr: acc.stderr + &item.stderr,
                        ..ShellResult::default()
                    }
                })
                .do_output()?;
        }
        SubCommand::Client(ClientCmd {
            client: client_id,
            sub: ClientSubCommand::Bash(ref cmd),
        }) => {
            let client = api_client.new_client_unchecked(&client_id);
            let flow = client
                .schedule_flow("Linux.Sys.BashShell", &cmd.command)
                .await?;
            log::debug!("Flow ID: {flow}");
            flow.fetch()
                .await?
                .into_iter()
                .fold::<ShellResult, _>(ShellResult::default(), |acc, item: ShellResult| {
                    ShellResult {
                        stdout: acc.stdout + &item.stdout,
                        stderr: acc.stderr + &item.stderr,
                        ..ShellResult::default()
                    }
                })
                .do_output()?;
        }
        SubCommand::Client(ClientCmd {
            client: client_id,
            sub: ClientSubCommand::Powershell(ref cmd),
        }) => {
            let client = api_client.new_client_unchecked(&client_id);
            let flow = client
                .schedule_flow("Windows.System.PowerShell", &cmd.command)
                .await?;
            log::debug!("Flow ID: {flow}");
            flow.fetch()
                .await?
                .into_iter()
                .fold::<ShellResult, _>(ShellResult::default(), |acc, item: ShellResult| {
                    ShellResult {
                        stdout: acc.stdout + &item.stdout,
                        stderr: acc.stderr + &item.stderr,
                        ..ShellResult::default()
                    }
                })
                .do_output()?;
        }
        SubCommand::Fetch(ref cmd) => {
            let buf = api_client.fetch(&cmd.path).await?;

            let mut output = std::fs::File::create(&cmd.output_file)?;
            output.write_all(&buf)?;
            output.flush()?;
        }
    }

    Ok(())
}
