//! E2E smoke test: spawn the real binary, wait for /health to return 200.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use url::Url;

// ── helpers ────────────────────────────────────────────────────────────────

/// Bind to 127.0.0.1:0 and return the OS-assigned port number.
fn pick_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind for port selection");
    listener.local_addr().unwrap().port()
}

/// Minimal raw HTTP/1.0 GET.  Returns the HTTP status code (e.g. 200) or an
/// error string.  Uses only stdlib so we have no async or heavy deps here.
fn ureq_get(raw_url: &str) -> Result<u16, String> {
    let url = Url::parse(raw_url).map_err(|e| format!("bad url: {e}"))?;
    let host = url.host_str().ok_or("no host")?;
    let port = url.port_or_known_default().ok_or("no port")?;
    let path = url.path();

    let addr = format!("{host}:{port}");
    let mut stream = TcpStream::connect(&addr).map_err(|e| format!("connect {addr}: {e}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();

    let request = format!("GET {path} HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write: {e}"))?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| format!("read: {e}"))?;

    // Parse the status line: "HTTP/1.x NNN ..."
    let status_line = response.lines().next().ok_or("empty response")?;
    let status_str = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| format!("malformed status line: {status_line}"))?;
    status_str
        .parse::<u16>()
        .map_err(|e| format!("status parse: {e}"))
}

/// Poll `url` until a 200 response is received or `deadline` is reached.
/// Returns `true` if we got 200 in time.
fn wait_for_200(url: &str, deadline: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if let Ok(200) = ureq_get(url) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    false
}

/// RAII guard that kills `child` on drop so the test process never leaks.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// ── test ───────────────────────────────────────────────────────────────────

#[test]
fn binary_starts_serves_health_and_exits_cleanly() {
    let port = pick_free_port();
    let data_dir = TempDir::new().expect("TempDir");

    let child = Command::new(env!("CARGO_BIN_EXE_community-search"))
        .env("COMMUNITY_SEARCH_BIND_ADDR", "127.0.0.1")
        .env("COMMUNITY_SEARCH_PORT", port.to_string())
        .env("COMMUNITY_SEARCH_DATA_DIR", data_dir.path())
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("failed to spawn community-search binary");

    let _guard = ChildGuard(child);

    let health_url = format!("http://127.0.0.1:{port}/health");
    let up = wait_for_200(&health_url, Duration::from_secs(10));

    assert!(
        up,
        "/health did not return 200 within 10 seconds on port {port}"
    );
}
