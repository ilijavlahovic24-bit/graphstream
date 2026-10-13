use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use sharding::ShardCluster;

use crate::collector::{collect_cluster, register_help};
use crate::registry::Registry;

/// Serve Prometheus `/metrics` on `addr` (e.g. `"0.0.0.0:9090"`).
///
/// Routes:
/// * `GET /metrics` — Prometheus exposition format
/// * `GET /healthz` — 200 OK with body `ok`
/// * `GET /readyz` — 200 OK if the cluster has at least one shard
pub async fn serve(
    addr: &str,
    cluster: Arc<tokio::sync::RwLock<ShardCluster>>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("metrics server listening on {addr}");

    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("accept failed: {e}");
                continue;
            }
        };
        let c = cluster.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(&mut stream, c).await {
                tracing::warn!("client error: {e}");
            }
        });
    }
}

async fn handle(
    stream: &mut tokio::net::TcpStream,
    cluster: Arc<tokio::sync::RwLock<ShardCluster>>,
) -> anyhow::Result<()> {
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).await?;
    if n == 0 { return Ok(()); }

    let req = String::from_utf8_lossy(&buf[..n]);
    let first_line = req.lines().next().unwrap_or("");
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("/");

    if method != "GET" {
        respond(stream, 405, "text/plain", "method not allowed").await?;
        return Ok(());
    }

    match path {
        "/metrics" => {
            let registry = Registry::new();
            register_help(&registry);
            {
                let guard = cluster.read().await;
                collect_cluster(&registry, &guard);
            }
            let body = registry.render();
            respond(stream, 200, "text/plain; version=0.0.4", &body).await?;
        }
        "/healthz" => {
            respond(stream, 200, "text/plain", "ok").await?;
        }
        "/readyz" => {
            let ready = cluster.read().await.shard_count() > 0;
            if ready {
                respond(stream, 200, "text/plain", "ready").await?;
            } else {
                respond(stream, 503, "text/plain", "no shards").await?;
            }
        }
        _ => respond(stream, 404, "text/plain", "not found").await?,
    }

    Ok(())
}

async fn respond(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
) -> anyhow::Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        503 => "Service Unavailable",
        _ => "Unknown",
    };
    let headers = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {len}\r\n\
         Cache-Control: no-store\r\n\
         Connection: close\r\n\
         \r\n",
        len = body.as_bytes().len()
    );
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}