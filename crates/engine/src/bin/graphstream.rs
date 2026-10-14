use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, NaiveDateTime};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use engine::repl::Repl;
use engine::Server;
use query::{DistributedQueryEngine, QueryEngine};
use sharding::ShardCluster;
use temporal_graph::TemporalGraph;
use visualization::{ControlAction, Mode, Sampler};

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

    /// Snapshot and visualization tools.
    Viz {
        #[command(subcommand)]
        action: VizAction,
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

#[derive(Subcommand)]
enum VizAction {
    /// Print a Graphviz DOT snapshot to stdout.
    Dot {
        /// Timestamp: ISO 8601 (`2024-01-01T00:00:05`) or ms since epoch.
        #[arg(long)]
        at: Option<String>,
    },

    /// Print a JSON snapshot to stdout.
    Json {
        #[arg(long)]
        at: Option<String>,
    },

    /// Serve the interactive realtime visualizer.
    Serve {
        #[arg(long, default_value = "7475")]
        port: u16,

        /// Initial mode: "live" or "playback".
        #[arg(long, default_value = "playback")]
        mode: String,

        /// Initial playback speed multiplier.
        #[arg(long, default_value = "100.0")]
        speed: f64,

        /// Start playing automatically in playback mode.
        #[arg(long, default_value = "false")]
        autoplay: bool,

        /// Tick interval in milliseconds.
        #[arg(long, default_value = "200")]
        tick_ms: u64,
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

        Command::Viz { action } => run_viz(action, graph).await,

        Command::Shard { .. } => unreachable!("handled above"),
    }
}

// ---- visualization ----------------------------------------------------

async fn run_viz(action: VizAction, graph: Arc<TemporalGraph>) -> anyhow::Result<()> {
    match action {
        VizAction::Dot { at } => {
            let t = parse_time_opt(at.as_deref(), &graph);
            let snap = visualization::summary::snapshot_graph(&graph, t);
            print!("{}", visualization::dot::render(&snap));
            Ok(())
        }

        VizAction::Json { at } => {
            let t = parse_time_opt(at.as_deref(), &graph);
            let snap = visualization::summary::snapshot_graph(&graph, t);
            println!("{}", serde_json::to_string_pretty(&snap)?);
            Ok(())
        }

        VizAction::Serve {
            port,
            mode,
            speed,
            autoplay,
            tick_ms,
        } => {
            let sampler = Arc::new(
                Sampler::new(graph.clone())
                    .with_tick_interval(Duration::from_millis(tick_ms))
                    .with_default_speed(speed),
            );

            let initial_mode = if mode.eq_ignore_ascii_case("live") {
                Mode::Live
            } else {
                Mode::Playback
            };

            sampler
                .apply(ControlAction::SetMode { mode: initial_mode })
                .map_err(|e| anyhow::anyhow!("invalid initial mode: {e}"))?;
            sampler
                .apply(ControlAction::SetSpeed { value: speed })
                .map_err(|e| anyhow::anyhow!("invalid initial speed: {e}"))?;

            if autoplay && initial_mode == Mode::Playback {
                sampler
                    .apply(ControlAction::Play)
                    .map_err(|e| anyhow::anyhow!("autoplay failed: {e}"))?;
            }

            let addr = format!("0.0.0.0:{port}");
            tracing::info!("visualizer running at http://localhost:{port}");

            let s = sampler.clone();
            tokio::spawn(async move {
                if let Err(e) = s.run().await {
                    tracing::error!("sampler stopped: {e}");
                }
            });

            visualization::server::serve(&addr, sampler).await
        }
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

// ---- helpers ----------------------------------------------------------

/// Parse a time argument from `--at`:
/// * `None` → latest timestamp seen in the graph, or wall clock if empty
/// * integer → milliseconds since Unix epoch
/// * ISO 8601 or `YYYY-MM-DD HH:MM:SS` → parsed timestamp
fn parse_time_opt(s: Option<&str>, graph: &TemporalGraph) -> NaiveDateTime {
    let Some(s) = s else {
        return latest_or_now(graph);
    };
    if let Ok(ms) = s.parse::<i64>() {
        return epoch() + ChronoDuration::milliseconds(ms);
    }
    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return dt;
        }
    }
    tracing::warn!(input = s, "unrecognized time format, using latest");
    latest_or_now(graph)
}

fn latest_or_now(graph: &TemporalGraph) -> NaiveDateTime {
    let mut latest: Option<NaiveDateTime> = None;
    for e in graph.edges() {
        for iv in e.intervals.iter() {
            latest = Some(match latest {
                Some(prev) if prev >= iv.end_time => prev,
                _ => iv.end_time,
            });
        }
    }
    latest.unwrap_or_else(|| chrono::Utc::now().naive_utc())
}

fn epoch() -> NaiveDateTime {
    chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}