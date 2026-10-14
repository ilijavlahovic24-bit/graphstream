use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::timeout;

use crate::control::{ControlAction, Mode, SamplerState};
use crate::snapshot::{epoch, ms, Snapshot};
use crate::summary::{snapshot_graph, SnapshotSource};

/// Maximum request body size for POST /control.
const MAX_BODY: usize = 64 * 1024;

/// Serve the visualizer + SSE stream.
pub async fn serve<S: SnapshotSource>(
    addr: &str,
    sampler: Arc<crate::sampler::Sampler<S>>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("visualizer listening on {addr}");

    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("accept failed: {e}");
                continue;
            }
        };
        let s = sampler.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(&mut stream, s).await {
                tracing::warn!("client error: {e}");
            }
        });
    }
}

async fn handle<S: SnapshotSource>(
    stream: &mut tokio::net::TcpStream,
    sampler: Arc<crate::sampler::Sampler<S>>,
) -> anyhow::Result<()> {
    // Read request line + headers + optional body.
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 4096];
    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 { return Ok(()); }
        buf.extend_from_slice(&tmp[..n]);

        if buf.len() > MAX_BODY {
            respond(stream, 413, "text/plain", "request too large").await?;
            return Ok(());
        }

        // Headers end at \r\n\r\n.
        if let Some(header_end) = find_headers_end(&buf) {
            let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
            let (method, path) = parse_request_line(&head);
            let content_length = parse_content_length(&head);

            let body_start = header_end + 4;
            if let Some(cl) = content_length {
                while buf.len() < body_start + cl {
                    let n = stream.read(&mut tmp).await?;
                    if n == 0 { break; }
                    buf.extend_from_slice(&tmp[..n]);
                }
            }
            let body = String::from_utf8_lossy(&buf[body_start..]).to_string();

            return route(stream, &method, &path, &body, sampler).await;
        }
    }
}

async fn route<S: SnapshotSource>(
    stream: &mut tokio::net::TcpStream,
    method: &str,
    path_with_query: &str,
    body: &str,
    sampler: Arc<crate::sampler::Sampler<S>>,
) -> anyhow::Result<()> {
    let (path, query) = match path_with_query.split_once('?') {
        Some((p, q)) => (p, q),
        None => (path_with_query, ""),
    };

    match (method, path) {
        ("GET", "/") | ("GET", "/viz") | ("GET", "/index.html") => {
            respond(stream, 200, "text/html; charset=utf-8", crate::html::HTML).await?;
        }

        ("GET", "/events") => {
            serve_sse(stream, sampler).await?;
        }

        ("GET", "/snapshot") => {
            let st = sampler.state();
            let t = parse_time_param(query)
                .unwrap_or_else(|| epoch() + chrono::Duration::milliseconds(st.virtual_time_ms));
            let snap = sampler.source().snapshot_at(t);
            let json = serde_json::to_string(&snap)?;
            respond(stream, 200, "application/json", &json).await?;
        }

        ("GET", "/meta") => {
            let st = sampler.state();
            let json = serde_json::to_string(&st)?;
            respond(stream, 200, "application/json", &json).await?;
        }

        ("POST", "/control") => {
            match serde_json::from_str::<ControlAction>(body) {
                Ok(action) => match sampler.apply(action) {
                    Ok(()) => {
                        let st = sampler.state();
                        let json = serde_json::to_string(&st)?;
                        respond(stream, 200, "application/json", &json).await?;
                    }
                    Err(e) => {
                        respond(stream, 400, "text/plain", &format!("invalid: {e}")).await?;
                    }
                },
                Err(e) => {
                    respond(stream, 400, "text/plain", &format!("bad JSON: {e}")).await?;
                }
            }
        }

        _ => {
            respond(stream, 404, "text/plain", "not found").await?;
        }
    }

    Ok(())
}

async fn serve_sse<S: SnapshotSource>(
    stream: &mut tokio::net::TcpStream,
    sampler: Arc<crate::sampler::Sampler<S>>,
) -> anyhow::Result<()> {
    // SSE headers.
    stream
        .write_all(
            b"HTTP/1.1 200 OK\r\n\
              Content-Type: text/event-stream\r\n\
              Cache-Control: no-cache\r\n\
              Connection: keep-alive\r\n\
              X-Accel-Buffering: no\r\n\
              \r\n",
        )
        .await?;
    stream.flush().await?;

    let (mut rx, _guard) = sampler.subscribe();

    // Send the initial state so the client can render controls immediately.
    let st = sampler.state();
    write_sse_event(stream, "meta", &serde_json::to_string(&st)?).await?;

    loop {
        // Heartbeat so intermediaries don't close idle connections.
        let msg = timeout(std::time::Duration::from_secs(15), rx.recv()).await;

        match msg {
            Ok(Ok(snap)) => {
                let payload = serialize_snapshot(&snap)?;
                write_sse_event(stream, "snapshot", &payload).await?;
            }
            Ok(Err(broadcast::error::RecvError::Lagged(n))) => {
                tracing::warn!("SSE client lagged {n} messages");
                // Fall through and continue receiving; the client will
                // just miss some intermediate frames.
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => break,
            Err(_) => {
                // Heartbeat.
                stream.write_all(b": keep-alive\n\n").await?;
                stream.flush().await?;
            }
        }
    }

    Ok(())
}

async fn write_sse_event(
    stream: &mut tokio::net::TcpStream,
    event: &str,
    data: &str,
) -> anyhow::Result<()> {
    stream.write_all(b"event: ").await?;
    stream.write_all(event.as_bytes()).await?;
    stream.write_all(b"\ndata: ").await?;
    // SSE requires no raw newlines in data; JSON serialization guarantees this.
    stream.write_all(data.as_bytes()).await?;
    stream.write_all(b"\n\n").await?;
    stream.flush().await?;
    Ok(())
}

#[derive(serde::Serialize)]
struct WireSnapshot<'a> {
    #[serde(flatten)]
    inner: &'a Snapshot,
    virtual_time_ms: i64,
    mode: Mode,
    playing: bool,
    speed: f64,
    loop_: bool,
    from_ms: i64,
    to_ms: i64,
}

fn serialize_snapshot(snap: &Snapshot) -> anyhow::Result<String> {
    // Enriched snapshot is built by the SSE handler from sampler state.
    // Here we only have the snapshot; the state travels in the `meta`
    // event, so we send the raw snapshot.
    Ok(serde_json::to_string(snap)?)
}

// ---- helpers ----------------------------------------------------------

fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn parse_request_line(head: &str) -> (String, String) {
    let line = head.lines().next().unwrap_or("");
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("/").to_string();
    (method, path)
}

fn parse_content_length(head: &str) -> Option<usize> {
    for line in head.lines().skip(1) {
        if let Some(rest) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            return rest.trim().parse().ok();
        }
    }
    None
}

fn parse_time_param(q: &str) -> Option<chrono::NaiveDateTime> {
    for pair in q.split('&') {
        let (k, v) = pair.split_once('=')?;
        if k == "t" {
            let ms_val: i64 = v.parse().ok()?;
            return Some(epoch() + chrono::Duration::milliseconds(ms_val));
        }
    }
    None
}

async fn respond(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
) -> anyhow::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_end_of_headers() {
        let buf = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\nbody";
        assert_eq!(find_headers_end(buf), Some(31));
    }

    #[test]
    fn parses_content_length_case_insensitive() {
        let head = "POST /control HTTP/1.1\r\nContent-Length: 42\r\n";
        assert_eq!(parse_content_length(head), Some(42));
    }

    #[test]
    fn parses_request_line() {
        let head = "GET /snapshot?t=100 HTTP/1.1\r\n";
        let (m, p) = parse_request_line(head);
        assert_eq!(m, "GET");
        assert_eq!(p, "/snapshot?t=100");
    }
}