//! HTML Link Extractor (Phase 61)
//!
//! Fetch HTML pages and extract all downloadable links with metadata.
//! Supports HTTP/HTTPS, FTP, Ed2k, and Magnet link extraction from HTML content.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// A link extracted from an HTML page
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtractedLink {
    /// The URL of the link
    pub url: String,
    /// Display text from the anchor tag (if any)
    pub text: Option<String>,
    /// Inferred filename from URL path or link text
    pub filename: Option<String>,
    /// Link protocol
    pub protocol: LinkProtocol,
    /// Whether the link has a `download` attribute
    pub has_download_attr: bool,
}

/// Protocol classification for extracted links
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum LinkProtocol {
    Http,
    Https,
    Ftp,
    Ed2k,
    Magnet,
    Unknown,
}

impl std::fmt::Display for LinkProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LinkProtocol::Http => write!(f, "http"),
            LinkProtocol::Https => write!(f, "https"),
            LinkProtocol::Ftp => write!(f, "ftp"),
            LinkProtocol::Ed2k => write!(f, "ed2k"),
            LinkProtocol::Magnet => write!(f, "magnet"),
            LinkProtocol::Unknown => write!(f, "unknown"),
        }
    }
}

/// Result of extracting links from an HTML page
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractionResult {
    /// Source URL that was scraped
    pub source_url: String,
    /// All extracted download links
    pub links: Vec<ExtractedLink>,
    /// Count by protocol
    pub protocol_counts: std::collections::HashMap<String, usize>,
}

/// Classify a URL into a protocol
pub fn classify_protocol(url: &str) -> LinkProtocol {
    let lower = url.to_lowercase();
    if lower.starts_with("https://") {
        LinkProtocol::Https
    } else if lower.starts_with("http://") {
        LinkProtocol::Http
    } else if lower.starts_with("ftp://") {
        LinkProtocol::Ftp
    } else if lower.starts_with("ed2k://") {
        LinkProtocol::Ed2k
    } else if lower.starts_with("magnet:") {
        LinkProtocol::Magnet
    } else {
        LinkProtocol::Unknown
    }
}

/// Extract the filename from a URL path
fn extract_filename_from_url(url: &str) -> Option<String> {
    // Try to parse as a standard URL
    if let Ok(parsed) = url::Url::parse(url) {
        let path = parsed.path();
        if let Some(last_segment) = path.rsplit('/').next() {
            if !last_segment.is_empty() {
                // URL-decode the filename
                return Some(
                    urlencoding::decode(last_segment)
                        .unwrap_or(std::borrow::Cow::Borrowed(last_segment))
                        .into_owned(),
                );
            }
        }
        return None;
    }

    // Fallback for non-standard URLs (ed2k, magnet)
    if let Some(pos) = url.rfind('/') {
        let name = &url[pos + 1..];
        if !name.is_empty() && name.len() < 256 {
            return Some(name.to_string());
        }
    }
    None
}

/// Extract filename from link text (look for patterns like "filename.ext")
fn extract_filename_from_text(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }

    // Search through all words for one that looks like a filename
    for word in trimmed.split_whitespace() {
        if word.contains('.') && !word.starts_with('.') && word.len() < 256 {
            // Check it has a reasonable extension
            if let Some(ext_pos) = word.rfind('.') {
                let ext = &word[ext_pos + 1..];
                if !ext.is_empty()
                    && ext.len() <= 10
                    && ext.chars().all(|c| c.is_ascii_alphanumeric())
                {
                    return Some(word.to_string());
                }
            }
        }
    }
    None
}

/// Extract all download links from HTML content
pub fn extract_links_from_html(html: &str, base_url: Option<&str>) -> Vec<ExtractedLink> {
    let mut links = Vec::new();
    let mut seen_urls = HashSet::new();

    // Extract <a ...>...</a> tags with href attributes
    // This regex handles common HTML patterns
    let anchor_re = regex_lite::Regex::new(
        r#"(?is)<a\s[^>]*?href\s*=\s*(?:"([^"]*)"|'([^']*)'|(\S+?))[^>]*?(?:\s+download(?:\s*=\s*(?:"[^"]*"|'[^']*')))?[^>]*?>(.*?)</a>"#
    ).unwrap();

    // Check for download attribute separately
    let download_attr_re = regex_lite::Regex::new(r"(?i)\bdownload\b").unwrap();

    for cap in anchor_re.captures_iter(html) {
        let href = cap
            .get(1)
            .or_else(|| cap.get(2))
            .or_else(|| cap.get(3))
            .map(|m| m.as_str().trim())
            .unwrap_or("");

        if href.is_empty() || href.starts_with('#') || href.starts_with("javascript:") {
            continue;
        }

        let link_text = cap.get(4).map(|m| strip_html_tags(m.as_str()));
        let full_match = cap.get(0).map(|m| m.as_str()).unwrap_or("");
        let has_download = download_attr_re.is_match(full_match);

        // Resolve relative URLs
        let resolved_url = resolve_url(href, base_url);
        if resolved_url.is_empty() {
            continue;
        }

        let protocol = classify_protocol(&resolved_url);
        if protocol == LinkProtocol::Unknown {
            continue;
        }

        // Deduplicate
        if !seen_urls.insert(resolved_url.clone()) {
            continue;
        }

        // Try to extract filename
        let filename = extract_filename_from_url(&resolved_url)
            .or_else(|| link_text.as_deref().and_then(extract_filename_from_text));

        links.push(ExtractedLink {
            url: resolved_url,
            text: link_text.filter(|t| !t.trim().is_empty()),
            filename,
            protocol,
            has_download_attr: has_download,
        });
    }

    // Also extract bare URLs from text content (outside of anchor tags)
    // for ed2k:// and magnet: links that may not be in <a> tags
    extract_bare_protocols(html, &mut links, &mut seen_urls);

    links
}

/// Extract bare protocol URLs from text (ed2k://, magnet:)
fn extract_bare_protocols(text: &str, links: &mut Vec<ExtractedLink>, seen: &mut HashSet<String>) {
    let protocols = ["ed2k://", "magnet:"];
    for proto in &protocols {
        let mut search_from = 0;
        while let Some(pos) = text[search_from..].find(proto) {
            let abs_pos = search_from + pos;
            let rest = &text[abs_pos..];
            let end = rest
                .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == '>' || c == ')')
                .unwrap_or(rest.len());
            let url = &rest[..end];

            if !url.is_empty() && seen.insert(url.to_string()) {
                let protocol = classify_protocol(url);
                let filename = extract_filename_from_url(url);
                links.push(ExtractedLink {
                    url: url.to_string(),
                    text: None,
                    filename,
                    protocol,
                    has_download_attr: false,
                });
            }

            search_from = abs_pos + proto.len();
        }
    }
}

/// Strip HTML tags from a string
fn strip_html_tags(html: &str) -> String {
    let tag_re = regex_lite::Regex::new(r"<[^>]+>").unwrap();
    let stripped = tag_re.replace_all(html, "");
    // Decode common HTML entities
    stripped
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
        .trim()
        .to_string()
}

/// Resolve a potentially relative URL against a base URL
fn resolve_url(href: &str, base_url: Option<&str>) -> String {
    // Already absolute
    if href.starts_with("http://")
        || href.starts_with("https://")
        || href.starts_with("ftp://")
        || href.starts_with("ed2k://")
        || href.starts_with("magnet:")
    {
        return href.to_string();
    }

    // Try to resolve relative URL
    if let Some(base) = base_url {
        if let Ok(base_parsed) = url::Url::parse(base) {
            if let Ok(resolved) = base_parsed.join(href) {
                return resolved.to_string();
            }
        }
    }

    String::new()
}

/// Generate a summary of extraction results
impl ExtractionResult {
    pub fn summary(&self) -> String {
        let total = self.links.len();
        let mut parts = vec![format!("Found {total} download link(s)")];

        for (proto, count) in &self.protocol_counts {
            parts.push(format!("{proto}: {count}"));
        }

        parts.join(", ")
    }

    /// Filter links by protocol
    pub fn links_by_protocol(&self, protocol: LinkProtocol) -> Vec<&ExtractedLink> {
        self.links
            .iter()
            .filter(|l| l.protocol == protocol)
            .collect()
    }

    /// Get only links that look like actual files (have filename extensions)
    pub fn file_links(&self) -> Vec<&ExtractedLink> {
        self.links
            .iter()
            .filter(|l| {
                l.filename
                    .as_ref()
                    .is_some_and(|f| f.contains('.') && !f.starts_with('.'))
            })
            .collect()
    }
}

/// Build an ExtractionResult from raw inputs
pub fn build_extraction_result(source_url: &str, links: Vec<ExtractedLink>) -> ExtractionResult {
    let mut protocol_counts = std::collections::HashMap::new();
    for link in &links {
        *protocol_counts
            .entry(link.protocol.to_string())
            .or_insert(0) += 1;
    }

    ExtractionResult {
        source_url: source_url.to_string(),
        links,
        protocol_counts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_protocol() {
        assert_eq!(
            classify_protocol("https://example.com/file.zip"),
            LinkProtocol::Https
        );
        assert_eq!(
            classify_protocol("http://example.com/file.zip"),
            LinkProtocol::Http
        );
        assert_eq!(
            classify_protocol("ftp://ftp.example.com/file.zip"),
            LinkProtocol::Ftp
        );
        assert_eq!(
            classify_protocol("ed2k://|file|test.avi|1234|abcd|/"),
            LinkProtocol::Ed2k
        );
        assert_eq!(
            classify_protocol("magnet:?xt=urn:btih:abc123"),
            LinkProtocol::Magnet
        );
        assert_eq!(classify_protocol("random://foo"), LinkProtocol::Unknown);
    }

    #[test]
    fn test_extract_filename_from_url() {
        assert_eq!(
            extract_filename_from_url("https://example.com/path/file.zip"),
            Some("file.zip".to_string())
        );
        assert_eq!(
            extract_filename_from_url("https://example.com/path/file%20name.tar.gz"),
            Some("file name.tar.gz".to_string())
        );
        assert_eq!(extract_filename_from_url("https://example.com/"), None);
        assert_eq!(extract_filename_from_url("https://example.com"), None);
    }

    #[test]
    fn test_extract_filename_from_text() {
        assert_eq!(
            extract_filename_from_text("Download file.zip here"),
            Some("file.zip".to_string())
        );
        assert_eq!(
            extract_filename_from_text("Get movie.mkv"),
            Some("movie.mkv".to_string())
        );
        assert_eq!(extract_filename_from_text("Click here"), None);
        assert_eq!(extract_filename_from_text(""), None);
    }

    #[test]
    fn test_extract_links_basic_html() {
        let html = r#"
            <html><body>
            <a href="https://example.com/file1.zip">Download ZIP</a>
            <a href="https://example.com/file2.tar.gz">Download TAR</a>
            <a href="ftp://ftp.example.com/pub/file3.iso">ISO Image</a>
            </body></html>
        "#;

        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 3);

        assert_eq!(links[0].url, "https://example.com/file1.zip");
        assert_eq!(links[0].text, Some("Download ZIP".to_string()));
        assert_eq!(links[0].filename, Some("file1.zip".to_string()));
        assert_eq!(links[0].protocol, LinkProtocol::Https);

        assert_eq!(links[1].url, "https://example.com/file2.tar.gz");
        assert_eq!(links[1].protocol, LinkProtocol::Https);

        assert_eq!(links[2].url, "ftp://ftp.example.com/pub/file3.iso");
        assert_eq!(links[2].protocol, LinkProtocol::Ftp);
    }

    #[test]
    fn test_extract_links_with_download_attr() {
        let html = r#"<a href="https://example.com/file.pdf" download>Get PDF</a>"#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 1);
        assert!(links[0].has_download_attr);
    }

    #[test]
    fn test_extract_links_skip_javascript_and_hash() {
        let html = r##"
            <a href="javascript:void(0)">Click</a>
            <a href="#section">Jump</a>
            <a href="https://example.com/real.zip">Real Link</a>
        "##;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].url, "https://example.com/real.zip");
    }

    #[test]
    fn test_extract_links_deduplication() {
        let html = r#"
            <a href="https://example.com/file.zip">Link 1</a>
            <a href="https://example.com/file.zip">Link 2</a>
        "#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 1);
    }

    #[test]
    fn test_extract_links_relative_urls() {
        let html = r#"<a href="/downloads/file.zip">Download</a>"#;
        let links = extract_links_from_html(html, Some("https://example.com"));
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].url, "https://example.com/downloads/file.zip");
    }

    #[test]
    fn test_extract_bare_ed2k_and_magnet() {
        let html = r#"
            <p>Download: ed2k://|file|test.avi|1234|abcd|/</p>
            <p>Or use magnet:?xt=urn:btih:abc123&dn=test</p>
        "#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].protocol, LinkProtocol::Ed2k);
        assert_eq!(links[1].protocol, LinkProtocol::Magnet);
    }

    #[test]
    fn test_strip_html_tags() {
        assert_eq!(strip_html_tags("<b>bold</b> text"), "bold text");
        assert_eq!(strip_html_tags("a &amp; b &lt; c"), "a & b < c");
    }

    #[test]
    fn test_resolve_url_absolute() {
        assert_eq!(
            resolve_url("https://example.com/file.zip", None),
            "https://example.com/file.zip"
        );
    }

    #[test]
    fn test_resolve_url_relative() {
        assert_eq!(
            resolve_url("/path/file.zip", Some("https://example.com")),
            "https://example.com/path/file.zip"
        );
    }

    #[test]
    fn test_resolve_url_no_base() {
        assert_eq!(resolve_url("/path/file.zip", None), "");
    }

    #[test]
    fn test_extraction_result_summary() {
        let links = vec![
            ExtractedLink {
                url: "https://example.com/a.zip".to_string(),
                text: None,
                filename: Some("a.zip".to_string()),
                protocol: LinkProtocol::Https,
                has_download_attr: false,
            },
            ExtractedLink {
                url: "ftp://ftp.example.com/b.iso".to_string(),
                text: None,
                filename: Some("b.iso".to_string()),
                protocol: LinkProtocol::Ftp,
                has_download_attr: false,
            },
        ];
        let result = build_extraction_result("https://example.com", links);
        assert_eq!(result.links.len(), 2);
        assert_eq!(*result.protocol_counts.get("https").unwrap(), 1);
        assert_eq!(*result.protocol_counts.get("ftp").unwrap(), 1);
        assert!(result.summary().contains("2 download link(s)"));
    }

    #[test]
    fn test_file_links_filter() {
        let links = vec![
            ExtractedLink {
                url: "https://example.com/file.zip".to_string(),
                text: None,
                filename: Some("file.zip".to_string()),
                protocol: LinkProtocol::Https,
                has_download_attr: false,
            },
            ExtractedLink {
                url: "https://example.com/page".to_string(),
                text: None,
                filename: None,
                protocol: LinkProtocol::Https,
                has_download_attr: false,
            },
        ];
        let result = build_extraction_result("https://example.com", links);
        assert_eq!(result.file_links().len(), 1);
    }

    #[test]
    fn test_links_by_protocol_filter() {
        let links = vec![
            ExtractedLink {
                url: "https://example.com/a.zip".to_string(),
                text: None,
                filename: Some("a.zip".to_string()),
                protocol: LinkProtocol::Https,
                has_download_attr: false,
            },
            ExtractedLink {
                url: "http://example.com/b.zip".to_string(),
                text: None,
                filename: Some("b.zip".to_string()),
                protocol: LinkProtocol::Http,
                has_download_attr: false,
            },
        ];
        let result = build_extraction_result("https://example.com", links);
        assert_eq!(result.links_by_protocol(LinkProtocol::Https).len(), 1);
        assert_eq!(result.links_by_protocol(LinkProtocol::Http).len(), 1);
        assert_eq!(result.links_by_protocol(LinkProtocol::Ftp).len(), 0);
    }

    #[test]
    fn test_extract_links_single_quotes() {
        let html = r#"<a href='https://example.com/file.zip'>Download</a>"#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].url, "https://example.com/file.zip");
    }

    #[test]
    fn test_extract_links_mixed_content() {
        let html = r#"
            <html><body>
            <h1>Downloads</h1>
            <a href="https://example.com/app.exe">Windows Installer</a>
            <a href="https://example.com/app.dmg">macOS Installer</a>
            <p>Or use ed2k://|file|app.bin|999|hash|/</p>
            <a href="magnet:?xt=urn:btih:abc">Magnet Link</a>
            </body></html>
        "#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 4);

        let result = build_extraction_result("https://example.com", links);
        assert_eq!(*result.protocol_counts.get("https").unwrap(), 2);
        assert_eq!(*result.protocol_counts.get("ed2k").unwrap(), 1);
        assert_eq!(*result.protocol_counts.get("magnet").unwrap(), 1);
    }
}
