use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use engine::repl::Repl;
use engine::Server;
use query::QueryEngine;
use temporal_graph::TemporalGraph;

#[derive(Parser)]
#[command(name = "graphstream", version, about = "GraphStream TQL engine")]
struct Cli {
    /// Path to the initial dataset (JSON)
    #[arg(long, global = true)]
    dataset: Option<PathBuf>,

    /// Maximum graph size in MB
    #[arg(long, default_value = "1024", global = true)]
    max_memory_mb: usize,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the interactive REPL
    Repl,

    /// Execute a single .tql file and print the result
    Run {
        /// Path to the .tql file
        file: PathBuf,
    },

    /// Start the TCP server
    Serve {
        #[arg(long, default_value = "7474")]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();

    // Build the graph and optionally load the initial dataset.
    let mut graph = TemporalGraph::new(cli.max_memory_mb);
    if let Some(path) = &cli.dataset {
        let p = path.to_string_lossy().to_string();
        engine::ingestion::load_dataset(&mut graph, &p).await?;
    }
    let graph = Arc::new(graph);

    match cli.command {
        Command::Repl => {
            let mut repl = Repl::new(graph);
            repl.run()?;
        }
        Command::Run { file } => {
            let src = std::fs::read_to_string(&file)?;
            let engine = QueryEngine::new(graph);
            match engine.query(src.trim_end_matches(';')) {
                Ok(res) => {
                    for row in &res.rows {
                        let cells: Vec<String> = row.cells.iter().map(|c| format!("{:?}", c)).collect();
                        println!("{}", cells.join(" | "));
                    }
                    println!("({} rows)", res.rows.len());
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
        }
        Command::Serve { port } => {
            let query_engine = Arc::new(QueryEngine::new(graph));
            let server = Server::new(port, query_engine);
            server.run().await?;
        }
    }

    Ok(())
}