use std::path::PathBuf;
use std::sync::Arc;

use chrono::Duration as ChronoDuration;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use engine::repl::Repl;
use engine::Server;
use query::{DistributedQueryEngine, QueryEngine};
use sharding::ShardCluster;
use temporal_graph::TemporalGraph;

#[derive(Parser)]
#[command(name = "graphstream", version, about = "GraphStream TQL engine")]
struct Cli {
    /// Path to the initial dataset (JSON).
    #[arg(long, global = true)]
    dataset: Option<PathBuf>,

    /// Maximum graph size in MB.
    #[arg(long, default_value = "1024", global = true)]
    max_memory_mb: usize,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the interactive REPL.
    Repl,

    /// Execute a single .tql file and print the result.
    Run {
        /// Path to the .tql file.
        file: PathBuf,
    },

    /// Start the TCP query server.
    Serve {
        #[arg(long, default_value = "7474")]
        port: u16,
    },

    /// Run a single shard pod (used inside the Kubernetes StatefulSet).
    ///
    /// Starts two TCP servers: the query server on `--query-port` and the
    /// Prometheus metrics server on `--metrics-port`.
    Shard {
        #[arg(long, default_value = "7474")]
        query_port: u16,
        #[arg(long, default_value = "9090")]
        metrics_port: u16,
        #[arg(long, default_value = "3600")]
        shard_window_seconds: u64,
        #[arg(long, default_value = "5000")]
        allowed_lateness_ms: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    // The `shard` command does not load a dataset — it receives events
    // from Kafka. Handle it before building a TemporalGraph.
    if let Command::Shard {
        query_port,
        metrics_port,
        shard_window_seconds,
        allowed_lateness_ms,
    } = cli.command
    {
        return run_shard(
            query_port,
            metrics_port,
            shard_window_seconds,
            allowed_lateness_ms,
        )
            .await;
    }

    // All other commands build a single-node TemporalGraph.
    let mut graph = TemporalGraph::new(cli.max_memory_mb);
    if let Some(path) = &cli.dataset {
        let p = path.to_string_lossy().to_string();
        tracing::info!(path = %p, "loading initial dataset");
        engine::ingestion::load_dataset(&mut graph, &p).await?;
    }
    let graph = Arc::new(graph);

    match cli.command {
        Command::Repl => {
            let mut repl = Repl::new(graph);
            repl.run()?;
            Ok(())
        }

        Command::Run { file } => {
            let src = std::fs::read_to_string(&file)?;
            let qe = QueryEngine::new(graph);
            match qe.query(src.trim_end_matches(';')) {
                Ok(res) => {
                    for row in &res.rows {
                        let cells: Vec<String> = row
                            .cells
                            .iter()
                            .map(|c| format!("{:?}", c))
                            .collect();
                        println!("{}", cells.join(" | "));
                    }
                    println!("({} rows)", res.rows.len());
                    Ok(())
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
        }

        Command::Serve { port } => {
            let qe = Arc::new(QueryEngine::new(graph));
            let server = Server::new(port, qe);
            tracing::info!("GraphStream query server on port {port}");
            server.run().await
        }

        Command::Shard { .. } => unreachable!("handled above"),
    }
}

// ---- shard pod (v3-2) -------------------------------------------------

async fn run_shard(
    query_port: u16,
    metrics_port: u16,
    shard_window_seconds: u64,
    allowed_lateness_ms: u64,
) -> anyhow::Result<()> {
    let cluster = Arc::new(tokio::sync::RwLock::new(ShardCluster::new(
        ChronoDuration::seconds(shard_window_seconds as i64),
        ChronoDuration::milliseconds(allowed_lateness_ms as i64),
        chrono::Utc::now().naive_utc(),
    )?));

    // Metrics server.
    {
        let c = cluster.clone();
        let addr = format!("0.0.0.0:{metrics_port}");
        tokio::spawn(async move {
            if let Err(e) = metrics::serve(&addr, c).await {
                tracing::error!("metrics server error: {e}");
            }
        });
    }

    // Query server.
    {
        let c = cluster.clone();
        let addr = format!("0.0.0.0:{query_port}");
        tokio::spawn(async move {
            if let Err(e) = run_shard_query_server(&addr, c).await {
                tracing::error!("query server error: {e}");
            }
        });
    }

    tracing::info!(
        query_port,
        metrics_port,
        shard_window_seconds,
        allowed_lateness_ms,
        "shard pod running"
    );

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received");
    Ok(())
}

/// Line-based TCP server for a shard pod.
///
/// Each incoming query snapshots the current cluster state, wraps it in a
/// `DistributedQueryEngine`, and runs the query. Snapshotting avoids
/// holding the cluster lock during query execution.
async fn run_shard_query_server(
    addr: &str,
    cluster: Arc<tokio::sync::RwLock<ShardCluster>>,
) -> anyhow::Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(addr).await?;
    tracing::info!("shard query server listening on {addr}");

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("accept failed: {e}");
                continue;
            }
        };
        let c = cluster.clone();
        tokio::spawn(async move {
            let (reader, mut writer) = stream.into_split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            let _ = writer
                .write_all(b"GraphStream shard query server\n")
                .await;
            let _ = writer
                .write_all(b"Send one query per line. Empty line disconnects.\n")
                .await;

            loop {
                line.clear();
                let n = match reader.read_line(&mut line).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                let _ = n;

                let tql = line.trim();
                if tql.is_empty() || tql == ":quit" || tql == ":exit" {
                    break;
                }

                let payload = {
                    let guard = c.read().await;
                    // Clone the cluster to release the lock before query
                    // execution.
                    let snapshot = guard.clone();
                    drop(guard);

                    let engine = DistributedQueryEngine::new(Arc::new(snapshot));
                    match engine.query(tql) {
                        Ok(res) => serde_json::json!({
                            "ok": true,
                            "columns": res.columns,
                            "rows": res.rows.iter()
                                .map(|r| r.cells.iter().map(|c| c.as_json()).collect::<Vec<_>>())
                                .collect::<Vec<_>>(),
                        }),
                        Err(e) => serde_json::json!({
                            "ok": false,
                            "error": e.to_string(),
                        }),
                    }
                };

                if writer
                    .write_all(payload.to_string().as_bytes())
                    .await
                    .is_err()
                {
                    break;
                }
                if writer.write_all(b"\n").await.is_err() {
                    break;
                }
            }
        });
    }
}