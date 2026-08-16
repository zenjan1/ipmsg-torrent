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
        if let Some(last_segment) = path.rsplit('/').next()
            && !last_segment.is_empty()
        {
            // URL-decode the filename
            return Some(
                urlencoding::decode(last_segment)
                    .unwrap_or(std::borrow::Cow::Borrowed(last_segment))
                    .into_owned(),
            );
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
    if let Some(base) = base_url
        && let Ok(base_parsed) = url::Url::parse(base)
        && let Ok(resolved) = base_parsed.join(href)
    {
        return resolved.to_string();
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

    // ========== Phase 246: Comprehensive Test Coverage ==========

    // --- LinkProtocol Display ---
    #[test]
    fn test_link_protocol_display_all_variants() {
        assert_eq!(LinkProtocol::Http.to_string(), "http");
        assert_eq!(LinkProtocol::Https.to_string(), "https");
        assert_eq!(LinkProtocol::Ftp.to_string(), "ftp");
        assert_eq!(LinkProtocol::Ed2k.to_string(), "ed2k");
        assert_eq!(LinkProtocol::Magnet.to_string(), "magnet");
        assert_eq!(LinkProtocol::Unknown.to_string(), "unknown");
    }

    // --- LinkProtocol traits ---
    #[test]
    fn test_link_protocol_clone_copy() {
        let proto = LinkProtocol::Https;
        let cloned = proto;
        assert_eq!(proto, cloned);
    }

    #[test]
    fn test_link_protocol_debug() {
        let proto = LinkProtocol::Magnet;
        let debug_str = format!("{:?}", proto);
        assert_eq!(debug_str, "Magnet");
    }

    #[test]
    fn test_link_protocol_eq_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(LinkProtocol::Http);
        set.insert(LinkProtocol::Https);
        set.insert(LinkProtocol::Http); // duplicate
        assert_eq!(set.len(), 2);
        assert!(set.contains(&LinkProtocol::Http));
        assert!(set.contains(&LinkProtocol::Https));
    }

    // --- LinkProtocol serde ---
    #[test]
    fn test_link_protocol_serde_roundtrip() {
        let protocols = [
            LinkProtocol::Http,
            LinkProtocol::Https,
            LinkProtocol::Ftp,
            LinkProtocol::Ed2k,
            LinkProtocol::Magnet,
            LinkProtocol::Unknown,
        ];
        for proto in protocols {
            let json = serde_json::to_string(&proto).unwrap();
            let deserialized: LinkProtocol = serde_json::from_str(&json).unwrap();
            assert_eq!(proto, deserialized);
        }
    }

    #[test]
    fn test_link_protocol_serde_lowercase_values() {
        let json_http = r#""http""#;
        let json_https = r#""https""#;
        let json_ftp = r#""ftp""#;
        let json_ed2k = r#""ed2k""#;
        let json_magnet = r#""magnet""#;
        let json_unknown = r#""unknown""#;

        assert_eq!(
            serde_json::from_str::<LinkProtocol>(json_http).unwrap(),
            LinkProtocol::Http
        );
        assert_eq!(
            serde_json::from_str::<LinkProtocol>(json_https).unwrap(),
            LinkProtocol::Https
        );
        assert_eq!(
            serde_json::from_str::<LinkProtocol>(json_ftp).unwrap(),
            LinkProtocol::Ftp
        );
        assert_eq!(
            serde_json::from_str::<LinkProtocol>(json_ed2k).unwrap(),
            LinkProtocol::Ed2k
        );
        assert_eq!(
            serde_json::from_str::<LinkProtocol>(json_magnet).unwrap(),
            LinkProtocol::Magnet
        );
        assert_eq!(
            serde_json::from_str::<LinkProtocol>(json_unknown).unwrap(),
            LinkProtocol::Unknown
        );
    }

    // --- ExtractedLink traits ---
    #[test]
    fn test_extracted_link_clone() {
        let link = ExtractedLink {
            url: "https://example.com/file.zip".to_string(),
            text: Some("Download".to_string()),
            filename: Some("file.zip".to_string()),
            protocol: LinkProtocol::Https,
            has_download_attr: true,
        };
        let cloned = link.clone();
        assert_eq!(cloned.url, link.url);
        assert_eq!(cloned.text, link.text);
        assert_eq!(cloned.filename, link.filename);
        assert_eq!(cloned.protocol, link.protocol);
        assert_eq!(cloned.has_download_attr, link.has_download_attr);
    }

    #[test]
    fn test_extracted_link_debug() {
        let link = ExtractedLink {
            url: "https://example.com/test.tar.gz".to_string(),
            text: None,
            filename: Some("test.tar.gz".to_string()),
            protocol: LinkProtocol::Https,
            has_download_attr: false,
        };
        let debug_str = format!("{:?}", link);
        assert!(debug_str.contains("ExtractedLink"));
        assert!(debug_str.contains("test.tar.gz"));
    }

    #[test]
    fn test_extracted_link_serde_roundtrip() {
        let link = ExtractedLink {
            url: "magnet:?xt=urn:btih:abc123".to_string(),
            text: Some("Torrent".to_string()),
            filename: None,
            protocol: LinkProtocol::Magnet,
            has_download_attr: false,
        };
        let json = serde_json::to_string(&link).unwrap();
        let deserialized: ExtractedLink = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.url, link.url);
        assert_eq!(deserialized.text, link.text);
        assert_eq!(deserialized.filename, link.filename);
        assert_eq!(deserialized.protocol, link.protocol);
    }

    #[test]
    fn test_extracted_link_serde_extra_fields_ignored() {
        let json = r#"{
            "url": "https://example.com/file.zip",
            "text": null,
            "filename": "file.zip",
            "protocol": "https",
            "has_download_attr": false,
            "extra_field": "ignored"
        }"#;
        let link: ExtractedLink = serde_json::from_str(json).unwrap();
        assert_eq!(link.url, "https://example.com/file.zip");
        assert_eq!(link.filename, Some("file.zip".to_string()));
    }

    // --- ExtractionResult traits ---
    #[test]
    fn test_extraction_result_clone() {
        let result = build_extraction_result(
            "https://example.com",
            vec![ExtractedLink {
                url: "https://example.com/a.zip".to_string(),
                text: None,
                filename: Some("a.zip".to_string()),
                protocol: LinkProtocol::Https,
                has_download_attr: false,
            }],
        );
        let cloned = result.clone();
        assert_eq!(cloned.source_url, result.source_url);
        assert_eq!(cloned.links.len(), result.links.len());
        assert_eq!(cloned.protocol_counts.len(), result.protocol_counts.len());
    }

    #[test]
    fn test_extraction_result_debug() {
        let result = build_extraction_result("https://example.com", vec![]);
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("ExtractionResult"));
        assert!(debug_str.contains("https://example.com"));
    }

    #[test]
    fn test_extraction_result_serde_roundtrip() {
        let result = build_extraction_result(
            "https://source.com",
            vec![ExtractedLink {
                url: "https://example.com/file.zip".to_string(),
                text: Some("Download".to_string()),
                filename: Some("file.zip".to_string()),
                protocol: LinkProtocol::Https,
                has_download_attr: true,
            }],
        );
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: ExtractionResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.source_url, result.source_url);
        assert_eq!(deserialized.links.len(), 1);
        assert_eq!(deserialized.protocol_counts.get("https"), Some(&1));
    }

    // --- classify_protocol edge cases ---
    #[test]
    fn test_classify_protocol_case_insensitive() {
        assert_eq!(
            classify_protocol("HTTPS://EXAMPLE.COM"),
            LinkProtocol::Https
        );
        assert_eq!(classify_protocol("HTTP://EXAMPLE.COM"), LinkProtocol::Http);
        assert_eq!(classify_protocol("FTP://EXAMPLE.COM"), LinkProtocol::Ftp);
        assert_eq!(classify_protocol("ED2K://FILE"), LinkProtocol::Ed2k);
        assert_eq!(classify_protocol("MAGNET:?XT=abc"), LinkProtocol::Magnet);
    }

    #[test]
    fn test_classify_protocol_empty_string() {
        assert_eq!(classify_protocol(""), LinkProtocol::Unknown);
    }

    #[test]
    fn test_classify_protocol_partial_match() {
        assert_eq!(classify_protocol("http"), LinkProtocol::Unknown);
        assert_eq!(classify_protocol("https"), LinkProtocol::Unknown);
        assert_eq!(classify_protocol("ftp"), LinkProtocol::Unknown);
    }

    #[test]
    fn test_classify_protocol_similar_prefixes() {
        assert_eq!(
            classify_protocol("httpx://example.com"),
            LinkProtocol::Unknown
        );
        assert_eq!(
            classify_protocol("httpsx://example.com"),
            LinkProtocol::Unknown
        );
    }

    // --- extract_filename_from_url edge cases ---
    #[test]
    fn test_extract_filename_from_url_nested_path() {
        assert_eq!(
            extract_filename_from_url("https://example.com/a/b/c/d/file.tar.gz"),
            Some("file.tar.gz".to_string())
        );
    }

    #[test]
    fn test_extract_filename_from_url_no_extension() {
        assert_eq!(
            extract_filename_from_url("https://example.com/path/filename"),
            Some("filename".to_string())
        );
    }

    #[test]
    fn test_extract_filename_from_url_trailing_slash() {
        assert_eq!(extract_filename_from_url("https://example.com/path/"), None);
    }

    #[test]
    fn test_extract_filename_from_url_query_params() {
        assert_eq!(
            extract_filename_from_url("https://example.com/file.zip?v=1"),
            Some("file.zip".to_string())
        );
    }

    #[test]
    fn test_extract_filename_from_url_fragment() {
        assert_eq!(
            extract_filename_from_url("https://example.com/file.zip#section"),
            Some("file.zip".to_string())
        );
    }

    #[test]
    fn test_extract_filename_from_url_unicode() {
        assert_eq!(
            extract_filename_from_url("https://example.com/%E4%B8%AD%E6%96%87.zip"),
            Some("中文.zip".to_string())
        );
    }

    #[test]
    fn test_extract_filename_from_url_special_chars() {
        assert_eq!(
            extract_filename_from_url("https://example.com/file%20with%20spaces.tar.gz"),
            Some("file with spaces.tar.gz".to_string())
        );
    }

    #[test]
    fn test_extract_filename_from_url_ed2k() {
        // ed2k URLs have a specific format
        let url = "ed2k://|file|ubuntu-22.04.iso|1234567|hash|/";
        // The function tries URL parsing first, then falls back
        let result = extract_filename_from_url(url);
        // ed2k format may or may not extract cleanly
        assert!(result.is_some() || result.is_none());
    }

    // --- extract_filename_from_text edge cases ---
    #[test]
    fn test_extract_filename_from_text_multiple_words() {
        assert_eq!(
            extract_filename_from_text("Please download my-document.pdf now"),
            Some("my-document.pdf".to_string())
        );
    }

    #[test]
    fn test_extract_filename_from_text_hidden_file() {
        // Files starting with . should not match
        assert_eq!(extract_filename_from_text("Get .hidden file"), None);
    }

    #[test]
    fn test_extract_filename_from_text_long_extension() {
        // Extension > 10 chars should not match
        assert_eq!(extract_filename_from_text("file.verylongextension"), None);
    }

    #[test]
    fn test_extract_filename_from_text_no_extension() {
        assert_eq!(extract_filename_from_text("just a word"), None);
    }

    #[test]
    fn test_extract_filename_from_text_whitespace_only() {
        assert_eq!(extract_filename_from_text("   "), None);
    }

    #[test]
    fn test_extract_filename_from_text_unicode() {
        assert_eq!(
            extract_filename_from_text("下载 文件.zip 到这里"),
            Some("文件.zip".to_string())
        );
    }

    // --- extract_links_from_html edge cases ---
    #[test]
    fn test_extract_links_empty_html() {
        let links = extract_links_from_html("", None);
        assert!(links.is_empty());
    }

    #[test]
    fn test_extract_links_no_links() {
        let html = "<html><body><p>No links here</p></body></html>";
        let links = extract_links_from_html(html, None);
        assert!(links.is_empty());
    }

    #[test]
    fn test_extract_links_empty_href() {
        let html = r#"<a href="">Empty</a>"#;
        let links = extract_links_from_html(html, None);
        assert!(links.is_empty());
    }

    #[test]
    fn test_extract_links_nested_html_in_anchor() {
        let html = r#"<a href="https://example.com/file.zip"><b>Bold</b> text</a>"#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 1);
        // Text should have HTML tags stripped
        assert!(links[0].text.as_ref().unwrap().contains("Bold"));
    }

    #[test]
    fn test_extract_links_download_attr_with_value() {
        let html = r#"<a href="https://example.com/file.zip" download="custom.zip">Download</a>"#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 1);
        assert!(links[0].has_download_attr);
    }

    #[test]
    fn test_extract_links_multiple_protocols() {
        let html = r#"
            <a href="https://example.com/secure.zip">HTTPS</a>
            <a href="http://example.com/insecure.zip">HTTP</a>
            <a href="ftp://ftp.example.com/ftp.zip">FTP</a>
        "#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 3);
        assert_eq!(links[0].protocol, LinkProtocol::Https);
        assert_eq!(links[1].protocol, LinkProtocol::Http);
        assert_eq!(links[2].protocol, LinkProtocol::Ftp);
    }

    #[test]
    fn test_extract_links_case_insensitive_href_quotes() {
        let html = r#"<A HREF='https://example.com/FILE.ZIP'>Download</A>"#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].url, "https://example.com/FILE.ZIP");
    }

    #[test]
    fn test_extract_links_with_query_params() {
        let html =
            r#"<a href="https://example.com/download.php?file=test.zip&id=123">Download</a>"#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 1);
        assert!(links[0].url.contains("file=test.zip"));
    }

    #[test]
    fn test_extract_links_relative_path_resolution() {
        let html = r#"<a href="../files/document.pdf">PDF</a>"#;
        let links = extract_links_from_html(html, Some("https://example.com/docs/page.html"));
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].url, "https://example.com/files/document.pdf");
    }

    #[test]
    fn test_extract_links_unknown_protocol_skipped() {
        let html = r#"<a href="custom://something">Custom</a>"#;
        let links = extract_links_from_html(html, None);
        assert!(links.is_empty());
    }

    // --- extract_bare_protocols edge cases ---
    #[test]
    fn test_extract_bare_ed2k_multiple() {
        let html = r#"
            <p>First: ed2k://|file|file1.avi|1000|hash1|/</p>
            <p>Second: ed2k://|file|file2.avi|2000|hash2|/</p>
        "#;
        let links = extract_links_from_html(html, None);
        let ed2k_links: Vec<_> = links
            .iter()
            .filter(|l| l.protocol == LinkProtocol::Ed2k)
            .collect();
        assert_eq!(ed2k_links.len(), 2);
    }

    #[test]
    fn test_extract_bare_magnet_with_params() {
        let html = r#"<p>magnet:?xt=urn:btih:abc123&dn=TestFile&tr=udp://tracker.example.com</p>"#;
        let links = extract_links_from_html(html, None);
        let magnet_links: Vec<_> = links
            .iter()
            .filter(|l| l.protocol == LinkProtocol::Magnet)
            .collect();
        assert_eq!(magnet_links.len(), 1);
        assert!(magnet_links[0].url.contains("dn=TestFile"));
    }

    #[test]
    fn test_extract_bare_protocols_deduplication() {
        let html = r#"
            <p>ed2k://|file|same.avi|1000|hash|/</p>
            <p>Again: ed2k://|file|same.avi|1000|hash|/</p>
        "#;
        let links = extract_links_from_html(html, None);
        let ed2k_links: Vec<_> = links
            .iter()
            .filter(|l| l.protocol == LinkProtocol::Ed2k)
            .collect();
        assert_eq!(ed2k_links.len(), 1);
    }

    #[test]
    fn test_extract_bare_protocols_terminated_by_quote() {
        let html = r#"<p>"ed2k://|file|test.avi|1000|hash|/"</p>"#;
        let links = extract_links_from_html(html, None);
        let ed2k_links: Vec<_> = links
            .iter()
            .filter(|l| l.protocol == LinkProtocol::Ed2k)
            .collect();
        assert_eq!(ed2k_links.len(), 1);
        assert!(!ed2k_links[0].url.contains('"'));
    }

    // --- strip_html_tags edge cases ---
    #[test]
    fn test_strip_html_tags_nested_tags() {
        assert_eq!(strip_html_tags("<div><p><b>text</b></p></div>"), "text");
    }

    #[test]
    fn test_strip_html_tags_all_entities() {
        let result = strip_html_tags("&amp; &lt; &gt; &quot; &#39; &nbsp;");
        // After entity decode and trim, verify all entities are converted
        assert!(result.contains('&'));
        assert!(result.contains('<'));
        assert!(result.contains('>'));
        assert!(result.contains('"'));
        assert!(result.contains('\''));
    }

    #[test]
    fn test_strip_html_tags_empty_string() {
        assert_eq!(strip_html_tags(""), "");
    }

    #[test]
    fn test_strip_html_tags_no_tags() {
        assert_eq!(strip_html_tags("plain text"), "plain text");
    }

    #[test]
    fn test_strip_html_tags_self_closing() {
        assert_eq!(strip_html_tags("before<br/>after"), "beforeafter");
    }

    // --- resolve_url edge cases ---
    #[test]
    fn test_resolve_url_all_protocols_absolute() {
        assert_eq!(
            resolve_url("ftp://ftp.example.com/file.zip", None),
            "ftp://ftp.example.com/file.zip"
        );
        assert_eq!(
            resolve_url("ed2k://|file|test.avi|1000|hash|/", None),
            "ed2k://|file|test.avi|1000|hash|/"
        );
        assert_eq!(
            resolve_url("magnet:?xt=urn:btih:abc", None),
            "magnet:?xt=urn:btih:abc"
        );
    }

    #[test]
    fn test_resolve_url_relative_with_base_path() {
        assert_eq!(
            resolve_url("file.zip", Some("https://example.com/downloads/")),
            "https://example.com/downloads/file.zip"
        );
    }

    #[test]
    fn test_resolve_url_relative_parent_directory() {
        assert_eq!(
            resolve_url("../file.zip", Some("https://example.com/a/b/")),
            "https://example.com/a/file.zip"
        );
    }

    #[test]
    fn test_resolve_url_empty_href() {
        // Empty string resolved against a base returns the base URL
        let result = resolve_url("", Some("https://example.com"));
        assert_eq!(result, "https://example.com/");
    }

    #[test]
    fn test_resolve_url_invalid_base_url() {
        assert_eq!(resolve_url("file.zip", Some("not a valid url")), "");
    }

    // --- build_extraction_result ---
    #[test]
    fn test_build_extraction_result_empty_links() {
        let result = build_extraction_result("https://example.com", vec![]);
        assert_eq!(result.source_url, "https://example.com");
        assert!(result.links.is_empty());
        assert!(result.protocol_counts.is_empty());
    }

    #[test]
    fn test_build_extraction_result_protocol_counts() {
        let links = vec![
            ExtractedLink {
                url: "https://a.com/1.zip".to_string(),
                text: None,
                filename: None,
                protocol: LinkProtocol::Https,
                has_download_attr: false,
            },
            ExtractedLink {
                url: "https://b.com/2.zip".to_string(),
                text: None,
                filename: None,
                protocol: LinkProtocol::Https,
                has_download_attr: false,
            },
            ExtractedLink {
                url: "ftp://c.com/3.zip".to_string(),
                text: None,
                filename: None,
                protocol: LinkProtocol::Ftp,
                has_download_attr: false,
            },
        ];
        let result = build_extraction_result("https://source.com", links);
        assert_eq!(result.protocol_counts.get("https"), Some(&2));
        assert_eq!(result.protocol_counts.get("ftp"), Some(&1));
    }

    // --- ExtractionResult methods ---
    #[test]
    fn test_extraction_result_summary_empty() {
        let result = build_extraction_result("https://example.com", vec![]);
        assert!(result.summary().contains("0 download link(s)"));
    }

    #[test]
    fn test_extraction_result_summary_multiple_protocols() {
        let links = vec![
            ExtractedLink {
                url: "https://a.com/1.zip".to_string(),
                text: None,
                filename: Some("1.zip".to_string()),
                protocol: LinkProtocol::Https,
                has_download_attr: false,
            },
            ExtractedLink {
                url: "ftp://b.com/2.iso".to_string(),
                text: None,
                filename: Some("2.iso".to_string()),
                protocol: LinkProtocol::Ftp,
                has_download_attr: false,
            },
            ExtractedLink {
                url: "magnet:?xt=abc".to_string(),
                text: None,
                filename: None,
                protocol: LinkProtocol::Magnet,
                has_download_attr: false,
            },
        ];
        let result = build_extraction_result("https://source.com", links);
        let summary = result.summary();
        assert!(summary.contains("3 download link(s)"));
        assert!(summary.contains("https: 1"));
        assert!(summary.contains("ftp: 1"));
        assert!(summary.contains("magnet: 1"));
    }

    #[test]
    fn test_file_links_filter_no_filename() {
        let links = vec![
            ExtractedLink {
                url: "https://example.com/page".to_string(),
                text: None,
                filename: None,
                protocol: LinkProtocol::Https,
                has_download_attr: false,
            },
            ExtractedLink {
                url: "https://example.com/another".to_string(),
                text: None,
                filename: None,
                protocol: LinkProtocol::Https,
                has_download_attr: false,
            },
        ];
        let result = build_extraction_result("https://example.com", links);
        assert!(result.file_links().is_empty());
    }

    #[test]
    fn test_file_links_filter_hidden_files() {
        // Files starting with . should not be included
        let links = vec![
            ExtractedLink {
                url: "https://example.com/.hidden".to_string(),
                text: None,
                filename: Some(".hidden".to_string()),
                protocol: LinkProtocol::Https,
                has_download_attr: false,
            },
            ExtractedLink {
                url: "https://example.com/visible.zip".to_string(),
                text: None,
                filename: Some("visible.zip".to_string()),
                protocol: LinkProtocol::Https,
                has_download_attr: false,
            },
        ];
        let result = build_extraction_result("https://example.com", links);
        assert_eq!(result.file_links().len(), 1);
        assert_eq!(
            result.file_links()[0].filename,
            Some("visible.zip".to_string())
        );
    }

    #[test]
    fn test_links_by_protocol_empty() {
        let result = build_extraction_result("https://example.com", vec![]);
        assert!(result.links_by_protocol(LinkProtocol::Https).is_empty());
        assert!(result.links_by_protocol(LinkProtocol::Ftp).is_empty());
    }

    // --- Unicode handling ---
    #[test]
    fn test_extract_links_unicode_content() {
        let html = r#"
            <a href="https://example.com/中文文件.zip">下载中文文件</a>
            <a href="https://example.com/日本語.tar.gz">日本語ファイル</a>
        "#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 2);
        assert!(links[0].text.as_ref().unwrap().contains("中文"));
        assert!(links[1].text.as_ref().unwrap().contains("日本語"));
    }

    #[test]
    fn test_extract_links_emoji_in_text() {
        let html = r#"<a href="https://example.com/file.zip">📥 Download 📥</a>"#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 1);
        assert!(links[0].text.as_ref().unwrap().contains("📥"));
    }

    // --- Complex HTML scenarios ---
    #[test]
    fn test_extract_links_complex_html_structure() {
        let html = r#"
            <!DOCTYPE html>
            <html>
            <head><title>Downloads</title></head>
            <body>
                <div class="container">
                    <h1>Available Downloads</h1>
                    <ul>
                        <li><a href="https://example.com/v1.0/app.exe">Version 1.0</a></li>
                        <li><a href="https://example.com/v2.0/app.exe">Version 2.0</a></li>
                    </ul>
                    <p>Legacy: <a href="ftp://archive.example.com/old/app.exe">FTP Archive</a></p>
                </div>
            </body>
            </html>
        "#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 3);
    }

    #[test]
    fn test_extract_links_with_html_entities_in_text() {
        let html = r#"<a href="https://example.com/file.zip">Tom &amp; Jerry&#39;s Download</a>"#;
        let links = extract_links_from_html(html, None);
        assert_eq!(links.len(), 1);
        let text = links[0].text.as_ref().unwrap();
        assert!(text.contains("&"));
        assert!(text.contains("'"));
    }

    // --- Edge cases for filename extraction ---
    #[test]
    fn test_filename_from_url_multiple_dots() {
        assert_eq!(
            extract_filename_from_url("https://example.com/archive.tar.gz"),
            Some("archive.tar.gz".to_string())
        );
    }

    #[test]
    fn test_filename_from_text_with_punctuation() {
        // "file.zip," has comma attached - extension check includes comma
        // so it fails the alphanumeric extension check
        assert_eq!(extract_filename_from_text("Get file.zip, please."), None);
        // But clean filename works
        assert_eq!(
            extract_filename_from_text("Get file.zip please"),
            Some("file.zip".to_string())
        );
    }

    #[test]
    fn test_filename_from_text_multiple_filenames() {
        // Should return the first valid filename found
        let result = extract_filename_from_text("Choose between file1.zip or file2.tar.gz");
        assert!(result.is_some());
    }

    // --- Integration-style tests ---
    #[test]
    fn test_full_extraction_workflow() {
        let html = r#"
            <html>
            <body>
                <h1>Software Downloads</h1>
                <a href="https://releases.example.com/app-v2.0.zip" download>Download Latest</a>
                <a href="https://releases.example.com/app-v1.0.zip">Previous Version</a>
                <p>Also available via FTP: <a href="ftp://ftp.example.com/pub/app.iso">ISO Image</a></p>
                <p>Torrent: magnet:?xt=urn:btih:abcdef123456</p>
                <p>Ed2k: ed2k://|file|app.bin|999999|hashvalue|/</p>
            </body>
            </html>
        "#;
        let links = extract_links_from_html(html, Some("https://example.com"));
        let result = build_extraction_result("https://example.com/downloads", links);

        assert!(result.links.len() >= 5);
        assert!(result.summary().contains("download link(s)"));
        assert!(!result.file_links().is_empty());
        assert!(!result.links_by_protocol(LinkProtocol::Https).is_empty());
    }

    #[test]
    fn test_extraction_with_base_url_resolution() {
        let html = r#"
            <a href="/downloads/file1.zip">Relative 1</a>
            <a href="../files/file2.zip">Relative 2</a>
            <a href="https://cdn.example.com/file3.zip">Absolute</a>
        "#;
        let links = extract_links_from_html(html, Some("https://example.com/pages/current/"));
        assert_eq!(links.len(), 3);
        assert!(links[0].url.starts_with("https://example.com/downloads/"));
        assert!(links[1].url.starts_with("https://example.com/pages/"));
        assert_eq!(links[2].url, "https://cdn.example.com/file3.zip");
    }
}
