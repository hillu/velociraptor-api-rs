use std::io::Write;
use std::path::PathBuf;

use clap::Parser;

use velociraptor_api::{Client, ClientConfig, QueryOptions};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

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

async fn schedule_flow(
    client: &Client,
    client_id: &str,
    artifact: &str,
    cmd: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    #[derive(Deserialize)]
    struct Request {
        flow_id: String,
    }
    #[derive(Deserialize)]
    struct Submit {
        request: Request,
    }

    let env = vec![
        ("client_id".to_string(), client_id.to_string()),
        ("artifact".to_string(), artifact.to_string()),
        ("Command".to_string(), cmd.to_string()),
    ];
    let requests: Vec<Submit> = client
        .query(
            r#"SELECT
               collect_client(client_id=client_id,
                              artifacts=artifact,
                              env=dict(Command=Command))
               AS request
               FROM scope()"#,
            &QueryOptions::new()
                .env(env.as_slice())
                .org_id("".to_string())
                .build(),
        )
        .await?;
    Ok(requests[0].request.flow_id.clone())
}

async fn fetch_flow<T: DeserializeOwned>(
    client: &Client,
    client_id: &str,
    flow_id: &str,
) -> Result<Vec<T>, Box<dyn std::error::Error>> {
    let options = QueryOptions::new()
        .env(vec![
            ("client_id".into(), client_id.into()),
            ("flow_id".into(), flow_id.into()),
        ])
        .org_id("".to_string())
        .build();

    let mut result: Vec<T>;
    loop {
        result = client
            .query(
                r#"SELECT * from flow_results(client_id = client_id, flow_id = flow_id)"#,
                &options,
            )
            .await?;
        if result.is_empty() {
            use std::time::Duration;
            std::thread::sleep(Duration::from_millis(100));
            log::debug!("Retrying...");
        } else {
            return Ok(result);
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
struct ShellResult {
    #[serde(rename = "Stdout")]
    stdout: String,
    #[serde(rename = "Stderr")]
    stderr: String,
    #[serde(rename = "ReturnCode")]
    returncode: i32,
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

    let client = Client::try_from(&ClientConfig::from_yaml_file(cli.config.clone())?)?;

    match cli.sub {
        SubCommand::Query(ref cmd) => {
            let result = client
                .query::<serde_json::Value>(
                    &cmd.query,
                    &QueryOptions::new()
                        .env(cmd.env.as_ref())
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
            let flow_id =
                schedule_flow(&client, &client_id, "Generic.Client.VQL", &cmd.query).await?;
            log::debug!("Flow ID: {}", flow_id);
            let result: Vec<serde_json::Value> = fetch_flow(&client, &client_id, &flow_id).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        SubCommand::Client(ClientCmd {
            client: client_id,
            sub: ClientSubCommand::Cmd(ref cmd),
        }) => {
            let flow_id =
                schedule_flow(&client, &client_id, "Windows.System.CmdShell", &cmd.command).await?;
            log::debug!("Flow ID: {}", flow_id);
            fetch_flow(&client, &client_id, &flow_id)
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
            let flow_id =
                schedule_flow(&client, &client_id, "Linux.Sys.BashShell", &cmd.command).await?;
            log::debug!("Flow ID: {}", flow_id);
            fetch_flow(&client, &client_id, &flow_id)
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
            let flow_id = schedule_flow(
                &client,
                &client_id,
                "Windows.System.PowerShell",
                &cmd.command,
            )
            .await?;
            log::debug!("Flow ID: {}", flow_id);
            fetch_flow(&client, &client_id, &flow_id)
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
            let buf = client.fetch(&cmd.path).await?;

            let mut output = std::fs::File::create(&cmd.output_file)?;
            output.write_all(&buf)?;
            output.flush()?;
        }
    }

    Ok(())
}
