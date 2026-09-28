use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use query::QueryEngine;

/// Line-based TCP server.
///
/// Protocol:
/// * One TQL statement per line, terminated by `\n`.
/// * On success: one JSON object `{ "ok": true, "columns": [...], "rows": [...] }`.
/// * On error:   one JSON object `{ "ok": false, "error": "..." }`.
/// * An empty line, `:quit`, `:exit`, or EOF disconnects the client.
///
/// Example:
/// ```text
/// $ nc localhost 7474
/// MATCH (a:Host) RETURN a
/// {"ok":true,"columns":["a"],"rows":[[{"id":0,"label":"Host","properties":{}}]]}
/// ```
pub struct Server {
    port: u16,
    query_engine: Arc<QueryEngine>,
}

impl Server {
    pub fn new(port: u16, query_engine: Arc<QueryEngine>) -> Self {
        Self { port, query_engine }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let addr = format!("0.0.0.0:{}", self.port);
        let listener = TcpListener::bind(&addr).await?;
        tracing::info!("GraphStream listening on {addr}");

        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!("accept failed: {e}");
                    continue;
                }
            };
            let engine = Arc::clone(&self.query_engine);
            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, engine).await {
                    tracing::warn!("client {peer} error: {e}");
                }
            });
        }
    }
}

async fn handle_client(
    stream: tokio::net::TcpStream,
    engine: Arc<QueryEngine>,
) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    writer.write_all(b"GraphStream TQL server\n").await?;
    writer.write_all(b"Send one query per line. Empty line to disconnect.\n").await?;

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 { break; } // EOF

        let tql = line.trim();
        if tql.is_empty() || tql == ":quit" || tql == ":exit" {
            break;
        }

        let payload = match engine.query(tql) {
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
        };

        writer.write_all(payload.to_string().as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }

    Ok(())
}