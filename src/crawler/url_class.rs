use url::Url;

/// Query-string parameters that carry tracking/attribution data only and never
/// affect what content the server returns. Stripped during normalization so
/// `https://x/y?utm_source=twitter` and `https://x/y` dedupe to one URL.
const TRACKING_PARAMS: &[&str] = &[
    // Google / Urchin
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "utm_name",
    "utm_reader",
    "utm_referrer",
    "gclid",
    "dclid",
    "_ga",
    // Facebook / Meta
    "fbclid",
    // Microsoft / Bing / LinkedIn
    "msclkid",
    "li_fat_id",
    // Yandex / TikTok / Mailchimp / HubSpot
    "yclid",
    "ttclid",
    "mc_cid",
    "mc_eid",
    "_hsenc",
    "_hsmi",
    "__hssc",
    "__hstc",
    "hsCtaTracking",
    // Generic referral / social
    "ref",
    "ref_src",
    "ref_url",
    "igshid",
    "oly_anon_id",
    "oly_enc_id",
    "wickedid",
    "guce_referrer",
];

fn is_tracking_param(name: &str) -> bool {
    TRACKING_PARAMS.iter().any(|p| p.eq_ignore_ascii_case(name))
}

/// Canonicalize a URL by:
/// - Parsing it (scheme and host are lowercased by the `url` crate per WHATWG spec)
/// - Stripping the fragment
/// - Clearing userinfo (username and password)
/// - Dropping default ports (80 for http, 443 for https)
/// - Removing tracking-only query parameters (see [`TRACKING_PARAMS`])
/// - Sorting remaining query parameters alphabetically by key so order-only
///   variants dedupe to the same URL
///
/// Returns `None` if the input cannot be parsed as a URL.
pub fn normalize_url(input: &str) -> Option<String> {
    let mut url = Url::parse(input).ok()?;

    // Strip fragment
    url.set_fragment(None);

    // Clear userinfo (errors only for schemes that don't support credentials, e.g. data:)
    let _ = url.set_username("");
    let _ = url.set_password(None);

    // Drop default ports.  The WHATWG URL spec already nulls them on parse, but
    // we enforce it explicitly for schemes we know about.
    if let Some(port) = url.port() {
        let is_default = matches!((url.scheme(), port), ("http", 80) | ("https", 443));
        if is_default {
            let _ = url.set_port(None);
        }
    }

    // Filter out tracking params and sort the remainder.
    // We always rewrite the query so order-only differences canonicalize equal.
    if url.query().is_some() {
        let mut kept: Vec<(String, String)> = url
            .query_pairs()
            .filter(|(k, _)| !is_tracking_param(k))
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        kept.sort_by(|a, b| a.0.cmp(&b.0));
        if kept.is_empty() {
            url.set_query(None);
        } else {
            let mut serializer = url.query_pairs_mut();
            serializer.clear();
            for (k, v) in &kept {
                serializer.append_pair(k, v);
            }
            drop(serializer);
        }
    }

    Some(url.to_string())
}

/// Returns `true` if `url` falls under the given `prefix`.
///
/// Both inputs are normalized before comparison, and a leading `www.` on the
/// host is treated as equivalent to the bare host: a prefix of
/// `https://example.com/` matches `https://www.example.com/foo` and vice
/// versa. Scheme and path are still compared strictly — an http URL will
/// not match an https prefix.
pub fn is_in_prefix(url: &str, prefix: &str) -> bool {
    let n_url = match normalize_url(url) {
        Some(u) => strip_leading_www(&u),
        None => return false,
    };
    let n_prefix = match normalize_url(prefix) {
        Some(p) => strip_leading_www(&p),
        None => return false,
    };
    n_url.starts_with(&n_prefix)
}

/// Strip the `www.` from the host portion of a normalized URL.
///
/// `normalize_url` produces `scheme://host[:port]/path?query`, so the only
/// `://www.` occurrence is the scheme→host boundary. `replacen(_, _, 1)`
/// caps replacements at one to be safe: a path or query that itself contains
/// `://www.` (e.g. an open-graph redirect parameter) is left alone.
fn strip_leading_www(normalized_url: &str) -> String {
    normalized_url.replacen("://www.", "://", 1)
}

/// Hosts whose links are share buttons, image/avatar CDNs, video embeds, or
/// other infrastructure — not substantive destinations a future crawl target
/// would want to follow. Outlinks pointing at these are dropped at discovery
/// time so they never reach the admin review queue.
///
/// Match is exact-host or any subdomain (e.g. `www.facebook.com`,
/// `m.facebook.com` both match `facebook.com`; `i0.wp.com`, `s2.wp.com`
/// both match `wp.com`).
const BLACKLISTED_OUTLINK_HOSTS: &[&str] = &[
    // Social-share / social-embed targets.
    "bufferapp.com",
    "buffer.com",
    "facebook.com",
    "fb.com",
    "instagram.com",
    "linkedin.com",
    "mix.com",
    "odnoklassniki.ru",
    "ok.ru",
    "pinterest.com",
    "reddit.com",
    "t.co",
    "threads.net",
    "tiktok.com",
    "tumblr.com",
    "twitter.com",
    "vk.com",
    "whatsapp.com",
    "x.com",
    "xing.com",
    // Avatar / image / asset CDNs (no useful text content).
    "fbcdn.net",
    "giphy.com",
    "googleusercontent.com",
    "gravatar.com",
    "imgur.com",
    "substackcdn.com",
    "twimg.com",
    "wp.com",
    // Video hosts — text crawler can't index the content.
    "youtube.com",
    "youtu.be",
];

/// Extract the lowercase host of `url`, with a leading `www.` stripped.
///
/// `www.` is treated as a presentation convention rather than a real
/// subdomain — so `www.example.com` and `example.com` both return
/// `example.com`. This collapses the most common outlink dup. Other
/// subdomains (`m.`, `en.`, `news.`) are preserved because they typically
/// identify distinct sites.
///
/// Returns `None` if the URL is unparseable or has no host (e.g. `mailto:`,
/// `data:`).
pub fn host_of(url: &str) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let host = parsed.host_str()?.to_ascii_lowercase();
    Some(host.strip_prefix("www.").unwrap_or(&host).to_string())
}

/// Returns `true` if `url`'s host is on the outlink blacklist (or a subdomain
/// of one). Used by the crawler to suppress social-share and similar links
/// from being recorded in `outlink_host_suggestions`.
pub fn is_blacklisted_outlink_host(url: &str) -> bool {
    let Ok(parsed) = Url::parse(url) else {
        return false;
    };
    let Some(host) = parsed.host_str() else {
        return false;
    };
    let host = host.to_ascii_lowercase();
    BLACKLISTED_OUTLINK_HOSTS
        .iter()
        .any(|target| host == *target || host.ends_with(&format!(".{target}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── normalize_url ──────────────────────────────────────────────────────────

    #[test]
    fn normalize_strips_fragment_and_lowercases_host() {
        let result = normalize_url("https://Example.COM/Path?q=1#frag");
        assert_eq!(result, Some("https://example.com/Path?q=1".to_string()));
    }

    #[test]
    fn normalize_drops_default_port() {
        // https default port 443
        let result = normalize_url("https://example.com:443/x");
        assert_eq!(result, Some("https://example.com/x".to_string()));

        // http default port 80
        let result = normalize_url("http://example.com:80/x");
        assert_eq!(result, Some("http://example.com/x".to_string()));
    }

    /// A bare-host URL and the same URL with a trailing slash must canonicalize
    /// identically — otherwise the crawler would store two `crawled_pages` rows
    /// for what is the same resource (the seed `https://example.com` and the
    /// home-page self-link `https://example.com/`).
    #[test]
    fn normalize_adds_trailing_slash_to_bare_host() {
        let a = normalize_url("https://example.com");
        let b = normalize_url("https://example.com/");
        assert_eq!(a, b, "bare host and trailing-slash must canonicalize equal");
        assert_eq!(a, Some("https://example.com/".to_string()));
    }

    #[test]
    fn normalize_strips_tracking_params() {
        let result =
            normalize_url("https://example.com/x?utm_source=twitter&utm_medium=social&q=hello");
        assert_eq!(result, Some("https://example.com/x?q=hello".to_string()));
    }

    #[test]
    fn normalize_drops_query_if_only_tracking_params() {
        let result = normalize_url("https://example.com/x?utm_source=twitter&fbclid=abc");
        assert_eq!(result, Some("https://example.com/x".to_string()));
    }

    #[test]
    fn normalize_sorts_remaining_query_params() {
        let a = normalize_url("https://example.com/x?b=2&a=1");
        let b = normalize_url("https://example.com/x?a=1&b=2");
        assert_eq!(a, b, "params in different orders must canonicalize equal");
        assert_eq!(a, Some("https://example.com/x?a=1&b=2".to_string()));
    }

    #[test]
    fn normalize_tracking_strip_is_case_insensitive() {
        let result = normalize_url("https://example.com/x?UTM_Source=twitter&q=hello");
        assert_eq!(result, Some("https://example.com/x?q=hello".to_string()));
    }

    #[test]
    fn normalize_preserves_value_only_params() {
        // Bare value-less keys (?foo) and empty-value keys (?foo=) are preserved.
        let result = normalize_url("https://example.com/x?foo&utm_source=z");
        assert_eq!(result, Some("https://example.com/x?foo=".to_string()));
    }

    // ── is_in_prefix ──────────────────────────────────────────────────────────

    #[test]
    fn in_prefix_matches_under_prefix() {
        assert!(is_in_prefix(
            "https://example.com/articles/post1",
            "https://example.com/articles/"
        ));
        assert!(is_in_prefix(
            "https://example.com/articles/sub/post2",
            "https://example.com/articles/"
        ));
    }

    #[test]
    fn in_prefix_rejects_different_host() {
        assert!(!is_in_prefix(
            "https://other.com/articles/post1",
            "https://example.com/articles/"
        ));
    }

    #[test]
    fn in_prefix_rejects_sibling_paths() {
        assert!(!is_in_prefix(
            "https://example.com/about",
            "https://example.com/articles/"
        ));
    }

    #[test]
    fn in_prefix_handles_fragment_in_url() {
        // Fragment is stripped by normalize_url before the starts_with check
        assert!(is_in_prefix(
            "https://example.com/articles/post#section",
            "https://example.com/articles/"
        ));
    }

    // ── is_blacklisted_outlink_host ───────────────────────────────────────────

    #[test]
    fn blacklist_matches_exact_host() {
        assert!(is_blacklisted_outlink_host("https://facebook.com/sharer"));
        assert!(is_blacklisted_outlink_host(
            "https://twitter.com/intent/tweet"
        ));
        assert!(is_blacklisted_outlink_host("https://vk.com/share.php"));
    }

    #[test]
    fn blacklist_matches_subdomain() {
        assert!(is_blacklisted_outlink_host("https://www.facebook.com/x"));
        assert!(is_blacklisted_outlink_host("https://m.facebook.com/y"));
        assert!(is_blacklisted_outlink_host(
            "https://business.linkedin.com/z"
        ));
    }

    #[test]
    fn blacklist_is_case_insensitive_on_host() {
        assert!(is_blacklisted_outlink_host("https://WWW.Facebook.COM/x"));
    }

    #[test]
    fn blacklist_does_not_match_unrelated_hosts() {
        assert!(!is_blacklisted_outlink_host("https://example.com/x"));
        assert!(!is_blacklisted_outlink_host(
            "https://news.ycombinator.com/"
        ));
    }

    /// A blacklisted token must not match a substring inside a different
    /// host — e.g. `notfacebook.com` is not on the list.
    #[test]
    fn blacklist_rejects_substring_lookalikes() {
        assert!(!is_blacklisted_outlink_host("https://notfacebook.com/x"));
        assert!(!is_blacklisted_outlink_host(
            "https://twitter.com.evil.example/x"
        ));
    }

    #[test]
    fn blacklist_handles_unparseable_url() {
        assert!(!is_blacklisted_outlink_host("not a url"));
    }

    #[test]
    fn blacklist_matches_new_cdn_and_video_entries() {
        // CDN/avatar/video entries added in the second blacklist pass.
        assert!(is_blacklisted_outlink_host(
            "https://gravatar.com/avatar/abc"
        ));
        assert!(is_blacklisted_outlink_host("https://i0.wp.com/img.png"));
        assert!(is_blacklisted_outlink_host("https://substackcdn.com/image"));
        assert!(is_blacklisted_outlink_host(
            "https://www.youtube.com/watch?v=x"
        ));
        assert!(is_blacklisted_outlink_host("https://youtu.be/abc"));
    }

    // ── host_of ───────────────────────────────────────────────────────────────

    #[test]
    fn host_of_strips_leading_www() {
        assert_eq!(
            host_of("https://www.example.com/x"),
            Some("example.com".into())
        );
        assert_eq!(host_of("https://example.com/x"), Some("example.com".into()));
    }

    #[test]
    fn host_of_does_not_strip_other_subdomains() {
        // Only the literal "www." prefix is collapsed; other subdomains are
        // typically distinct sites (en.wikipedia.org ≠ wikipedia.org).
        assert_eq!(
            host_of("https://m.example.com/x"),
            Some("m.example.com".into())
        );
        assert_eq!(
            host_of("https://en.wikipedia.org/x"),
            Some("en.wikipedia.org".into())
        );
    }

    #[test]
    fn host_of_lowercases() {
        assert_eq!(
            host_of("https://WWW.Example.COM/x"),
            Some("example.com".into())
        );
    }

    #[test]
    fn host_of_returns_none_for_hostless_urls() {
        // mailto: has no host in url::Url's sense.
        assert_eq!(host_of("mailto:user@example.com"), None);
        assert_eq!(host_of("not a url"), None);
    }

    // ── is_in_prefix www-collapsing ───────────────────────────────────────────

    #[test]
    fn in_prefix_treats_www_as_equivalent_to_bare_host() {
        // The motivating case: a crawl target prefix of https://lesswrong.com/
        // should match a discovered https://www.lesswrong.com/foo and vice
        // versa, so we don't double-store the same site.
        assert!(is_in_prefix(
            "https://www.lesswrong.com/posts/x",
            "https://lesswrong.com/"
        ));
        assert!(is_in_prefix(
            "https://lesswrong.com/posts/x",
            "https://www.lesswrong.com/"
        ));
        assert!(is_in_prefix(
            "https://www.lesswrong.com/posts/x",
            "https://www.lesswrong.com/"
        ));
    }

    #[test]
    fn in_prefix_does_not_collapse_other_subdomains() {
        // m.X and X stay distinct — only `www.` is treated as decorative.
        assert!(!is_in_prefix(
            "https://m.example.com/x",
            "https://example.com/"
        ));
        assert!(!is_in_prefix(
            "https://en.wikipedia.org/wiki/X",
            "https://wikipedia.org/"
        ));
    }

    #[test]
    fn in_prefix_still_requires_scheme_match() {
        // http URL must not match an https prefix even with www-collapse.
        assert!(!is_in_prefix(
            "http://www.example.com/x",
            "https://example.com/"
        ));
    }
}
