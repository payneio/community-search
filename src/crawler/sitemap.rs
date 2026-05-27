//! Sitemap discovery for crawl targets.
//!
//! Many sites — Substack-style blogs in particular — render their home pages
//! entirely client-side, so the crawler's `<a href>` extraction finds no
//! article links and the BFS terminates after one page. `sitemap.xml` exposes
//! the same content server-side and is the standard way to bootstrap a crawl
//! when the homepage isn't crawler-friendly.
//!
//! This module is best-effort: any transport, parse, or robots failure ends
//! with an empty result rather than propagating an error, so the caller can
//! always fall back to the BFS-only crawl.

use std::collections::HashSet;

use quick_xml::events::Event;
use quick_xml::reader::Reader;
use tracing::warn;
use url::Url;

use super::{
    fetcher::Fetcher,
    robots::RobotsChecker,
    url_class::{is_in_prefix, normalize_url},
};

/// Cap on the size of a single sitemap document we'll accept (10 MiB).
const MAX_SITEMAP_BYTES: usize = 10 * 1024 * 1024;
/// Cap on the number of sitemap documents we'll fetch for one target.
const MAX_SITEMAPS_PER_TARGET: usize = 20;

/// Discover in-prefix URLs from a target's sitemaps.
///
/// Looks at:
/// 1. `Sitemap:` directives in the target's robots.txt.
/// 2. The conventional `{scheme}://{host}/sitemap.xml`.
///
/// Sitemap-index files are followed one level deep. Discovered URLs are
/// normalized, filtered by `prefix`, robots-checked, deduplicated, and
/// capped at `max_urls`.
pub async fn discover_urls(
    seed_url: &str,
    prefix: &str,
    fetcher: &Fetcher,
    robots: &RobotsChecker,
    max_urls: usize,
) -> Vec<String> {
    if max_urls == 0 {
        return Vec::new();
    }

    let mut to_visit = collect_sitemap_candidates(seed_url, robots).await;
    if to_visit.is_empty() {
        return Vec::new();
    }
    to_visit.truncate(MAX_SITEMAPS_PER_TARGET);

    let mut found: HashSet<String> = HashSet::new();
    let mut visited_sitemaps: HashSet<String> = HashSet::new();
    let mut depth_remaining: usize = 1; // one level of sitemap-index recursion

    loop {
        let mut next_round: Vec<String> = Vec::new();
        for sm_url in to_visit.drain(..) {
            if !visited_sitemaps.insert(sm_url.clone()) {
                continue;
            }
            // Respect robots for the sitemap URL itself.
            if !robots.is_allowed(&sm_url).await.unwrap_or(true) {
                continue;
            }
            let body = match fetcher.fetch_text_raw(&sm_url, MAX_SITEMAP_BYTES).await {
                Ok(Some(body)) => body,
                _ => continue,
            };
            let parsed = match parse_sitemap(&body) {
                Ok(p) => p,
                Err(e) => {
                    warn!(sitemap = %sm_url, error = %e, "sitemap parse failed");
                    continue;
                }
            };
            match parsed {
                ParsedSitemap::Urls(urls) => {
                    for u in urls {
                        let Some(norm) = normalize_url(&u) else {
                            continue;
                        };
                        if !is_in_prefix(&norm, prefix) {
                            continue;
                        }
                        if !robots.is_allowed(&norm).await.unwrap_or(true) {
                            continue;
                        }
                        found.insert(norm);
                        if found.len() >= max_urls {
                            return found.into_iter().collect();
                        }
                    }
                }
                ParsedSitemap::Index(children) => {
                    for child in children {
                        if !visited_sitemaps.contains(&child)
                            && next_round.len() + visited_sitemaps.len() < MAX_SITEMAPS_PER_TARGET
                        {
                            next_round.push(child);
                        }
                    }
                }
            }
        }
        if next_round.is_empty() || depth_remaining == 0 {
            break;
        }
        depth_remaining -= 1;
        to_visit = next_round;
    }

    found.into_iter().collect()
}

async fn collect_sitemap_candidates(seed_url: &str, robots: &RobotsChecker) -> Vec<String> {
    let mut out: Vec<String> = robots.sitemaps_for(seed_url).await.unwrap_or_default();
    if let Some(default) = default_sitemap_url(seed_url) {
        if !out.iter().any(|s| s == &default) {
            out.push(default);
        }
    }
    out
}

fn default_sitemap_url(seed_url: &str) -> Option<String> {
    let parsed = Url::parse(seed_url).ok()?;
    let host = parsed.host_str()?;
    let scheme = parsed.scheme();
    let port = parsed.port().map(|p| format!(":{p}")).unwrap_or_default();
    Some(format!("{scheme}://{host}{port}/sitemap.xml"))
}

#[derive(Debug, PartialEq, Eq)]
enum ParsedSitemap {
    /// `<urlset>` — leaf sitemap; the strings are page URLs.
    Urls(Vec<String>),
    /// `<sitemapindex>` — the strings are URLs of further sitemap documents.
    Index(Vec<String>),
}

/// Parse a sitemap document into either page URLs or child-sitemap URLs.
///
/// Recognises both `<urlset>` and `<sitemapindex>` roots; collects every
/// `<loc>` body regardless of namespace prefix. Anything else is ignored.
fn parse_sitemap(body: &str) -> Result<ParsedSitemap, quick_xml::Error> {
    let mut reader = Reader::from_str(body);
    reader.config_mut().trim_text(true);

    let mut is_index = false;
    let mut root_seen = false;
    let mut in_loc = false;
    let mut locs: Vec<String> = Vec::new();

    loop {
        match reader.read_event()? {
            Event::Start(e) => {
                let qname = e.name();
                let local = local_name(qname.as_ref());
                if !root_seen {
                    if local == b"sitemapindex" {
                        is_index = true;
                    }
                    root_seen = true;
                }
                if local == b"loc" {
                    in_loc = true;
                }
            }
            Event::Text(e) => {
                if in_loc {
                    if let Ok(s) = e.unescape() {
                        let trimmed = s.trim();
                        if !trimmed.is_empty() {
                            locs.push(trimmed.to_string());
                        }
                    }
                }
            }
            Event::End(e) => {
                let qname = e.name();
                if local_name(qname.as_ref()) == b"loc" {
                    in_loc = false;
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(if is_index {
        ParsedSitemap::Index(locs)
    } else {
        ParsedSitemap::Urls(locs)
    })
}

/// Strip any namespace prefix from an XML qualified name.
/// `b"ns:loc"` → `b"loc"`; `b"loc"` → `b"loc"`.
fn local_name(qname: &[u8]) -> &[u8] {
    match qname.iter().position(|&c| c == b':') {
        Some(i) => &qname[i + 1..],
        None => qname,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_sitemap_url_uses_host_root() {
        assert_eq!(
            default_sitemap_url("https://www.example.com/blog/"),
            Some("https://www.example.com/sitemap.xml".to_string())
        );
        assert_eq!(
            default_sitemap_url("http://example.com:8080/"),
            Some("http://example.com:8080/sitemap.xml".to_string())
        );
    }

    #[test]
    fn parse_urlset_extracts_loc_entries() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>https://example.com/a</loc></url>
          <url><loc>https://example.com/b</loc><lastmod>2024-01-01</lastmod></url>
        </urlset>"#;
        let parsed = parse_sitemap(xml).expect("parse ok");
        assert_eq!(
            parsed,
            ParsedSitemap::Urls(vec![
                "https://example.com/a".to_string(),
                "https://example.com/b".to_string(),
            ])
        );
    }

    #[test]
    fn parse_sitemapindex_extracts_child_sitemaps() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <sitemap><loc>https://example.com/sitemap-1.xml</loc></sitemap>
          <sitemap><loc>https://example.com/sitemap-2.xml</loc></sitemap>
        </sitemapindex>"#;
        let parsed = parse_sitemap(xml).expect("parse ok");
        assert_eq!(
            parsed,
            ParsedSitemap::Index(vec![
                "https://example.com/sitemap-1.xml".to_string(),
                "https://example.com/sitemap-2.xml".to_string(),
            ])
        );
    }

    #[test]
    fn parse_handles_namespace_prefix_on_loc() {
        // Some sitemaps qualify element names with a prefix.
        let xml = r#"<?xml version="1.0"?>
        <ns:urlset xmlns:ns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <ns:url><ns:loc>https://example.com/a</ns:loc></ns:url>
        </ns:urlset>"#;
        let parsed = parse_sitemap(xml).expect("parse ok");
        assert_eq!(
            parsed,
            ParsedSitemap::Urls(vec!["https://example.com/a".to_string()])
        );
    }

    #[test]
    fn local_name_strips_prefix() {
        assert_eq!(local_name(b"ns:loc"), b"loc");
        assert_eq!(local_name(b"loc"), b"loc");
        assert_eq!(local_name(b""), b"");
    }
}
