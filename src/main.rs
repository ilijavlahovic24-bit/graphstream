use tracing_subscriber::fmt::init;
use clap::Parser;
use tracing::info;


#[derive(Parser)]
struct Config {
    /// Port that GraphStream listens
    #[arg(long, default_value = "7474")]
    port: u16,

    /// Maximum Graph Size in Mb
    #[arg(long, default_value = "1024")]
    max_memory_mb: usize,

    /// Path to initial dataset
    #[arg(long)]
    dataset: Option<String>,

    /// Whether it will be real-time visualization
    #[arg(long, default_value = "false")]
    visualization: bool,
}



#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logging initialisation
    tracing_subscriber::init();

    let config = Config::parse();

    info!(
        port = config.port,
        max_memory_mb = config.max_memory_mb,
        "Starting GraphStream engine"
    );

    let graph = TemporalGraph::new(config.max_memory_mb);


    if let Some(path) = config.dataset {
        info!(path = %path, "Loading initial dataset");
        engine::ingestion::load_dataset(&graph, &path).await?;
    }


    let query_engine = query::QueryEngine::new(graph.clone());


    if config.visualization {
        let viz = visualization::realtime::Sampler::new(graph.clone());
        tokio::spawn(async move {
            viz.run().await
        });
    }

    let server = engine::Server::new(config.port, query_engine);

    info!("GraphStream ready on port {}", config.port);

    server.run().await?;

    Ok(())
}