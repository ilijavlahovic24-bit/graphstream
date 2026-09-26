use std::sync::Arc;

use clap::Parser;
use tracing::info;

use query::QueryEngine;
use temporal_graph::TemporalGraph;
use visualization::Sampler;

#[derive(Parser)]
struct Config {
    /// Port that GraphStream listens on
    #[arg(long, default_value = "7474")]
    port: u16,

    /// Maximum graph size in MB
    #[arg(long, default_value = "1024")]
    max_memory_mb: usize,

    /// Path to initial dataset
    #[arg(long)]
    dataset: Option<String>,

    /// Enable real-time visualization
    #[arg(long, default_value = "false")]
    visualization: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logging
    tracing_subscriber::fmt::init();

    let config = Config::parse();

    info!(
        port = config.port,
        max_memory_mb = config.max_memory_mb,
        "Starting GraphStream engine"
    );

    // 1) Create a graph (mutable) and load it from the dataset before you load it
    //  share between components.
    let mut graph = TemporalGraph::new(config.max_memory_mb);

    if let Some(path) = &config.dataset {
        info!(path = %path, "Loading initial dataset");
        engine::ingestion::load_dataset(&mut graph, path).await?;
    }

    // 2) From this moment on, the graph is read-only and is shared through Arc.
    let graph = Arc::new(graph);

    // 3) Query engine gets its shared handle.
    let query_engine = Arc::new(QueryEngine::new(Arc::clone(&graph)));

    // 4) Optional visualization — spawns in the background.
    if config.visualization {
        let viz = Sampler::new(Arc::clone(&graph));
        tokio::spawn(async move {
            if let Err(e) = viz.run().await {
                tracing::error!("visualization error: {e}");
            }
        });
    }
    // 5) The server takes over the query engine and blocks until shutdown.
    let server = engine::Server::new(config.port, query_engine);

    info!("GraphStream ready on port {}", config.port);
    server.run().await?;

    Ok(())
}