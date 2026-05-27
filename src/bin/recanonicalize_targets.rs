//! One-off maintenance script: walk every row in `crawl_targets`, ask the
//! server at `url_prefix` what its canonical form is, and rewrite the row
//! in place when the answer differs.
//!
//! Stop the server before running. Reads from `data/data.sqlite` by default;
//! pass the path as the first arg to override.
//!
//! Usage:
//!   cargo run --bin recanonicalize_targets
//!   cargo run --bin recanonicalize_targets -- path/to/data.sqlite

use std::env;
use std::path::PathBuf;

use std::time::Duration;

use community_search::config::Config;
use community_search::crawler::canonical::detect_canonical_prefix;
use reqwest::redirect;
use rusqlite::Connection;
use url::Url;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_path: PathBuf = env::args()
        .nth(1)
        .unwrap_or_else(|| "data/data.sqlite".into())
        .into();

    // Use the same crawler User-Agent the running server would use, so sites
    // that 403/429 reqwest's default UA respond correctly here.
    let cfg = Config::load()?;
    let ua = cfg.crawler_user_agent.clone();
    println!("Using User-Agent: {ua}");

    println!("Opening {db_path:?}");
    let conn = Connection::open(&db_path)?;

    // Load all targets up front: we'll be doing HTTP calls inside the loop,
    // and we don't want to hold a prepared statement across awaits.
    let mut rows: Vec<(String, String)> = {
        let mut stmt = conn.prepare("SELECT id, url_prefix FROM crawl_targets ORDER BY url_prefix")?;
        let iter = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
        iter.collect::<Result<Vec<_>, _>>()?
    };
    rows.sort_by(|a, b| a.1.cmp(&b.1));

    println!("Checking {} crawl targets...", rows.len());
    let mut changed = 0usize;
    let mut unchanged = 0usize;
    let mut skipped_conflict = 0usize;

    // Build a client once for diagnostics; same redirect/timeout/UA as
    // detect_canonical_prefix so the explanation reflects what it would see.
    let client = reqwest::Client::builder()
        .redirect(redirect::Policy::limited(5))
        .timeout(Duration::from_secs(3))
        .user_agent(&ua)
        .build()?;

    for (id, current) in &rows {
        match detect_canonical_prefix(current, &ua).await {
            Some(canonical) if canonical != *current => {
                let res = conn.execute(
                    "UPDATE crawl_targets SET url_prefix = ?1 WHERE id = ?2",
                    rusqlite::params![canonical, id],
                );
                match res {
                    Ok(_) => {
                        println!("  ✓ {current}  →  {canonical}");
                        changed += 1;
                    }
                    Err(e) if e.to_string().contains("UNIQUE") => {
                        println!("  ⚠ {current}  →  {canonical}  (conflict — another target already has this prefix, leaving original)");
                        skipped_conflict += 1;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Some(_) | None => {
                // detect_canonical_prefix returned None — explain *why* so
                // the user can tell "already canonical" apart from
                // "rejected redirect" and "network error".
                let reason = diagnose(&client, current).await;
                println!("  · {current}  ({reason})");
                unchanged += 1;
            }
        }
    }

    println!();
    println!("Done. {changed} rewritten, {unchanged} unchanged, {skipped_conflict} skipped (conflict).");
    Ok(())
}

/// Repeat the request done by `detect_canonical_prefix` and explain what
/// happened. Diagnostic only — never modifies the DB.
async fn diagnose(client: &reqwest::Client, input: &str) -> String {
    let input_url = match Url::parse(input) {
        Ok(u) => u,
        Err(_) => return "unparseable URL".into(),
    };

    let resp = match client.get(input).send().await {
        Ok(r) => r,
        Err(e) if e.is_timeout() => return "timeout".into(),
        Err(e) if e.is_connect() => return "connect error".into(),
        Err(e) => return format!("network error: {e}"),
    };

    let final_url = resp.url().clone();
    let status = resp.status();
    if !status.is_success() {
        return format!("non-2xx final status: {status} at {final_url}");
    }
    if final_url.as_str() == input_url.as_str() {
        return "already canonical (no redirect)".into();
    }
    if final_url.path() != input_url.path()
        && final_url.path().trim_end_matches('/') != input_url.path().trim_end_matches('/')
    {
        return format!(
            "path-changing redirect rejected: {} → {}",
            input_url.path(),
            final_url.path()
        );
    }
    format!("redirected to {final_url} but detection still returned None (unexpected)")
}
