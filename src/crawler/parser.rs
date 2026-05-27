use scraper::{ElementRef, Html, Selector};
use url::Url;

/// A link extracted from an HTML page, with its resolved absolute URL and visible text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedLink {
    pub url: String,
    pub text: String,
}

/// Parse `html` and return all absolute HTTP/HTTPS links found in `<a href>` elements,
/// resolved against `base_url`.
///
/// Skips:
/// - `mailto:`, `javascript:`, `tel:` schemes
/// - Fragment-only hrefs (starting with `#`)
/// - Empty hrefs
/// - URLs whose scheme is not `http` or `https` after resolution
///
/// Returns an empty `Vec` if `base_url` cannot be parsed.
pub fn extract_links(html: &str, base_url: &str) -> Vec<ExtractedLink> {
    let base = match Url::parse(base_url) {
        Ok(u) => u,
        Err(_) => return Vec::new(),
    };

    let document = Html::parse_document(html);
    let selector = Selector::parse("a[href]").unwrap();

    let mut links = Vec::new();

    for el in document.select(&selector) {
        let href = match el.value().attr("href") {
            Some(h) => h.trim(),
            None => continue,
        };

        // Skip empty or fragment-only hrefs
        if href.is_empty() || href.starts_with('#') {
            continue;
        }

        // Skip non-navigational schemes (case-insensitive prefix check)
        let lower = href.to_lowercase();
        if lower.starts_with("mailto:")
            || lower.starts_with("javascript:")
            || lower.starts_with("tel:")
        {
            continue;
        }

        // Resolve against base URL; skip if resolution fails
        let resolved = match base.join(href) {
            Ok(u) => u,
            Err(_) => continue,
        };

        // Require http or https scheme
        if resolved.scheme() != "http" && resolved.scheme() != "https" {
            continue;
        }

        // Collect all inner text nodes, joining and trimming
        let text = el.text().collect::<String>();
        let text = text.trim().to_string();

        links.push(ExtractedLink {
            url: resolved.to_string(),
            text,
        });
    }

    links
}

/// Return the first `<link rel="canonical" href="...">` URL from `html`,
/// resolved against `base_url`. Returns `None` when no canonical link is
/// present, when the `href` is empty/unresolvable, or when the resolved
/// scheme is not http/https.
///
/// `rel` is an HTML token list, so we match `canonical` as a whole word —
/// e.g. `rel="canonical alternate"` is still a canonical link.
pub fn extract_canonical(html: &str, base_url: &str) -> Option<String> {
    let base = Url::parse(base_url).ok()?;
    let document = Html::parse_document(html);
    let selector = Selector::parse("link[rel][href]").ok()?;

    for el in document.select(&selector) {
        let rel = el.value().attr("rel")?;
        let has_canonical = rel
            .split_ascii_whitespace()
            .any(|tok| tok.eq_ignore_ascii_case("canonical"));
        if !has_canonical {
            continue;
        }
        let href = el.value().attr("href")?.trim();
        if href.is_empty() {
            continue;
        }
        let resolved = base.join(href).ok()?;
        if resolved.scheme() != "http" && resolved.scheme() != "https" {
            continue;
        }
        return Some(resolved.to_string());
    }
    None
}

/// Extracted content from a parsed HTML page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedPage {
    pub title: String,
    pub text: String,
}

/// Parse `html`, extracting the page title and visible body text.
///
/// Script, style, and noscript elements are excluded from the body text.
pub fn parse_text(html: &str) -> ParsedPage {
    let document = Html::parse_document(html);

    // ── title ────────────────────────────────────────────────────────────────
    let title_sel = Selector::parse("title").unwrap();
    let title = document
        .select(&title_sel)
        .next()
        .map(|el| el.text().collect::<String>().trim().to_string())
        .unwrap_or_default();

    // ── visible body text ────────────────────────────────────────────────────
    let body_sel = Selector::parse("body").unwrap();
    let drop_sel = Selector::parse("script, style, noscript").unwrap();

    let mut parts: Vec<String> = Vec::new();

    if let Some(body) = document.select(&body_sel).next() {
        for node in body.descendants() {
            if let Some(text) = node.value().as_text() {
                // Skip text nodes that live inside a dropped element.
                let in_drop = node
                    .ancestors()
                    .filter_map(ElementRef::wrap)
                    .any(|el| drop_sel.matches(&el));

                if !in_drop {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() {
                        parts.push(trimmed.to_string());
                    }
                }
            }
        }
    }

    ParsedPage {
        title,
        text: parts.join(" "),
    }
}

#[cfg(test)]
mod link_tests {
    use super::*;

    /// Relative paths are resolved against the base URL.
    /// Absolute URLs are preserved as-is.
    #[test]
    fn resolves_relative_links_against_base() {
        let html = r#"<!DOCTYPE html>
<html>
<body>
  <a href="/foo">Absolute path</a>
  <a href="bar">Relative path</a>
  <a href="https://other.org/page">Already absolute</a>
</body>
</html>"#;
        let links = extract_links(html, "https://example.com/dir/page.html");
        let urls: Vec<&str> = links.iter().map(|l| l.url.as_str()).collect();
        assert!(
            urls.contains(&"https://example.com/foo"),
            "/foo should resolve to https://example.com/foo, got: {:?}",
            urls
        );
        assert!(
            urls.contains(&"https://example.com/dir/bar"),
            "bar should resolve to https://example.com/dir/bar, got: {:?}",
            urls
        );
        assert!(
            urls.contains(&"https://other.org/page"),
            "absolute URL should be preserved, got: {:?}",
            urls
        );
    }

    /// mailto:, javascript:, tel:, fragment-only, and empty hrefs are all dropped.
    /// Only an https link survives.
    #[test]
    fn skips_non_http_schemes_and_fragments() {
        let html = r##"<!DOCTYPE html>
<html>
<body>
  <a href="mailto:foo@bar.com">Mail</a>
  <a href="javascript:void(0)">JS</a>
  <a href="tel:+1234567890">Phone</a>
  <a href="#anchor">Fragment</a>
  <a href="">Empty</a>
  <a href="https://example.com/valid">Valid</a>
</body>
</html>"##;
        let links = extract_links(html, "https://example.com/");
        assert_eq!(
            links.len(),
            1,
            "only the https link should survive, got: {:?}",
            links
        );
        assert_eq!(links[0].url, "https://example.com/valid");
    }

    /// Inner text of nested elements is concatenated and trimmed.
    #[test]
    fn captures_link_text() {
        let html = r#"<!DOCTYPE html>
<html>
<body>
  <a href="https://example.com/bold"><b>Bold</b> text</a>
</body>
</html>"#;
        let links = extract_links(html, "https://example.com/");
        assert_eq!(links.len(), 1);
        assert_eq!(
            links[0].text, "Bold text",
            "nested text should be concatenated, got: {:?}",
            links[0].text
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Title is extracted correctly; visible headings and paragraphs appear in body.
    #[test]
    fn extracts_title_and_body_text() {
        let html = r#"<!DOCTYPE html>
<html>
<head><title>Hello World</title></head>
<body>
    <h1>Heading</h1>
    <p>Some body text.</p>
</body>
</html>"#;
        let page = parse_text(html);
        assert_eq!(page.title, "Hello World");
        assert!(
            page.text.contains("Heading"),
            "body text should contain 'Heading', got: {:?}",
            page.text
        );
        assert!(
            page.text.contains("Some body text."),
            "body text should contain 'Some body text.', got: {:?}",
            page.text
        );
    }

    /// script/style content is excluded; regular paragraph text survives.
    #[test]
    fn strips_script_and_style_content() {
        let html = r#"<!DOCTYPE html>
<html>
<head>
    <style>body { color: red; }</style>
    <title>Test</title>
</head>
<body>
    <script>alert('hello');</script>
    <p>visible</p>
    <noscript>no js</noscript>
</body>
</html>"#;
        let page = parse_text(html);
        assert!(
            !page.text.contains("alert"),
            "'alert' must not appear in body text, got: {:?}",
            page.text
        );
        assert!(
            !page.text.contains("color: red"),
            "'color: red' must not appear in body text, got: {:?}",
            page.text
        );
        assert!(
            page.text.contains("visible"),
            "body text should contain 'visible', got: {:?}",
            page.text
        );
    }

    #[test]
    fn canonical_extracts_absolute_url() {
        let html = r#"<!DOCTYPE html>
<html><head>
<link rel="canonical" href="https://example.com/canonical-page">
</head><body>x</body></html>"#;
        let canon = extract_canonical(html, "https://example.com/aliased?utm_source=z");
        assert_eq!(canon.as_deref(), Some("https://example.com/canonical-page"));
    }

    #[test]
    fn canonical_resolves_relative_href() {
        let html = r#"<!DOCTYPE html>
<html><head>
<link rel="canonical" href="/canonical-page">
</head><body>x</body></html>"#;
        let canon = extract_canonical(html, "https://example.com/dir/aliased");
        assert_eq!(canon.as_deref(), Some("https://example.com/canonical-page"));
    }

    #[test]
    fn canonical_matches_word_in_rel_token_list() {
        let html = r#"<!DOCTYPE html>
<html><head>
<link rel="alternate canonical" href="https://example.com/c">
</head><body>x</body></html>"#;
        let canon = extract_canonical(html, "https://example.com/aliased");
        assert_eq!(canon.as_deref(), Some("https://example.com/c"));
    }

    #[test]
    fn canonical_missing_returns_none() {
        let html = r#"<!DOCTYPE html><html><head></head><body>x</body></html>"#;
        assert!(extract_canonical(html, "https://example.com/x").is_none());
    }

    #[test]
    fn canonical_skips_non_http_schemes() {
        let html = r#"<!DOCTYPE html>
<html><head>
<link rel="canonical" href="mailto:foo@bar.com">
</head><body>x</body></html>"#;
        assert!(extract_canonical(html, "https://example.com/x").is_none());
    }

    /// No <title> tag → empty string; body text is still extracted.
    #[test]
    fn empty_title_when_missing() {
        let html = r#"<!DOCTYPE html>
<html>
<body><p>Some content</p></body>
</html>"#;
        let page = parse_text(html);
        assert_eq!(page.title, "", "title should be empty when not present");
        assert!(
            page.text.contains("Some content"),
            "body text should still be extracted, got: {:?}",
            page.text
        );
    }
}
