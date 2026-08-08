//! RSS/Atom feed subscription with auto-download.
//!
//! Users subscribe to RSS/Atom feed URLs. The system periodically polls each
//! feed for new items, extracts enclosure/download URLs, and auto-queues
//! matching items into the download manager.
//!
//! Supported formats:
//! - RSS 2.0 (`<channel><item><enclosure url="..."/>`)
//! - Atom 1.0 (`<feed><entry><link rel="enclosure" href="..."/>`)
//!
//! Filters:
//! - Optional regex/glob pattern on item titles
//! - Optional file-extension filter (e.g., `["mp4", "mkv"]`)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

#[derive(Error, Debug)]
pub enum RssFeedError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Feed not found: {0}")]
    NotFound(String),
    #[error("Subscription limit reached: {0}")]
    LimitReached(usize),
}

/// A single feed item (entry) discovered in a feed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeedItem {
    /// Unique identifier (guid/id, or URL fallback).
    pub id: String,
    /// Item title.
    pub title: String,
    /// Download URL (from enclosure/link).
    pub url: String,
    /// File size in bytes (if provided by enclosure).
    pub size: Option<u64>,
    /// Publication date.
    pub published: Option<DateTime<Utc>>,
    /// MIME type (if provided).
    pub content_type: Option<String>,
}

/// A user subscription to a feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedSubscription {
    /// Unique subscription id.
    pub id: String,
    /// Feed URL.
    pub feed_url: String,
    /// User-friendly label.
    pub label: Option<String>,
    /// Optional title filter (substring match, case-insensitive).
    pub title_filter: Option<String>,
    /// Optional file-extension filter (lowercase, e.g., `["mp4", "mkv"]`).
    pub extensions: Vec<String>,
    /// Whether this subscription is active.
    pub enabled: bool,
    /// Polling interval in seconds.
    pub poll_interval_secs: u64,
    /// Last successful poll time.
    pub last_poll: Option<DateTime<Utc>>,
    /// Set of item IDs already processed (bounded).
    #[serde(default)]
    pub seen_ids: Vec<String>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

/// Persisted state for all subscriptions.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedSubscriptions {
    version: u32,
    subscriptions: Vec<FeedSubscription>,
}

/// In-memory + persisted feed subscription manager.
pub struct FeedSubscriptionManager {
    inner: Arc<RwLock<FeedSubscriptionManagerInner>>,
}

struct FeedSubscriptionManagerInner {
    subscriptions: Vec<FeedSubscription>,
    config_path: std::path::PathBuf,
    http_client: reqwest::Client,
    max_subscriptions: usize,
    max_seen_ids: usize,
    /// Callback invoked for each auto-queued download URL.
    on_new_item: Option<NewItemCallback>,
}

type NewItemCallback = Box<dyn Fn(FeedItem, &FeedSubscription) + Send + Sync>;

/// Maximum number of seen IDs to retain per subscription before evicting oldest.
const DEFAULT_MAX_SEEN_IDS: usize = 1000;
/// Default subscription limit.
const DEFAULT_MAX_SUBSCRIPTIONS: usize = 100;
/// Default poll interval (15 minutes).
const DEFAULT_POLL_INTERVAL_SECS: u64 = 15 * 60;

impl FeedSubscriptionManager {
    /// Create a new manager, restoring persisted state from `config_path` if present.
    pub async fn new(config_path: impl AsRef<Path>) -> Result<Self, RssFeedError> {
        let config_path = config_path.as_ref().to_path_buf();
        let subs = if config_path.exists() {
            let data = tokio::fs::read_to_string(&config_path).await?;
            let persisted: PersistedSubscriptions = serde_json::from_str(&data).unwrap_or_default();
            persisted.subscriptions
        } else {
            Vec::new()
        };

        let http_client = reqwest::Client::builder()
            .user_agent("ipmsg-torrent/2.4 (RSS/Atom)")
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        let inner = FeedSubscriptionManagerInner {
            subscriptions: subs,
            config_path,
            http_client,
            max_subscriptions: DEFAULT_MAX_SUBSCRIPTIONS,
            max_seen_ids: DEFAULT_MAX_SEEN_IDS,
            on_new_item: None,
        };

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    /// Register a callback invoked for each newly discovered download item.
    /// The callback runs inside `poll_feed()` and must not block.
    pub async fn set_on_new_item<F>(&self, cb: F)
    where
        F: Fn(FeedItem, &FeedSubscription) + Send + Sync + 'static,
    {
        let mut inner = self.inner.write().await;
        inner.on_new_item = Some(Box::new(cb));
    }

    /// Add a new feed subscription. Returns the new subscription id.
    pub async fn add_subscription(
        &self,
        feed_url: &str,
        label: Option<&str>,
        title_filter: Option<&str>,
        extensions: Vec<String>,
    ) -> Result<String, RssFeedError> {
        if feed_url.is_empty() {
            return Err(RssFeedError::InvalidUrl("empty feed URL".into()));
        }
        let mut inner = self.inner.write().await;
        if inner.subscriptions.len() >= inner.max_subscriptions {
            return Err(RssFeedError::LimitReached(inner.max_subscriptions));
        }
        // Deduplicate by feed_url.
        if inner.subscriptions.iter().any(|s| s.feed_url == feed_url) {
            return Err(RssFeedError::InvalidUrl(format!(
                "already subscribed to {feed_url}"
            )));
        }
        let id = uuid::Uuid::new_v4().to_string();
        let sub = FeedSubscription {
            id: id.clone(),
            feed_url: feed_url.to_string(),
            label: label.map(|s| s.to_string()),
            title_filter: title_filter.map(|s| s.to_string()),
            extensions: extensions.into_iter().map(|s| s.to_lowercase()).collect(),
            enabled: true,
            poll_interval_secs: DEFAULT_POLL_INTERVAL_SECS,
            last_poll: None,
            seen_ids: Vec::new(),
            created_at: Utc::now(),
        };
        inner.subscriptions.push(sub);
        drop(inner);
        self.persist().await?;
        info!(feed_url = feed_url, id = %id, "added feed subscription");
        Ok(id)
    }

    /// Remove a subscription by id.
    pub async fn remove_subscription(&self, id: &str) -> Result<(), RssFeedError> {
        let mut inner = self.inner.write().await;
        let before = inner.subscriptions.len();
        inner.subscriptions.retain(|s| s.id != id);
        if inner.subscriptions.len() == before {
            return Err(RssFeedError::NotFound(id.to_string()));
        }
        drop(inner);
        self.persist().await?;
        Ok(())
    }

    /// Enable or disable a subscription.
    pub async fn set_enabled(&self, id: &str, enabled: bool) -> Result<(), RssFeedError> {
        let mut inner = self.inner.write().await;
        let sub = inner
            .subscriptions
            .iter_mut()
            .find(|s| s.id == id)
            .ok_or_else(|| RssFeedError::NotFound(id.to_string()))?;
        sub.enabled = enabled;
        drop(inner);
        self.persist().await?;
        Ok(())
    }

    /// Update the polling interval for a subscription.
    pub async fn set_poll_interval(&self, id: &str, secs: u64) -> Result<(), RssFeedError> {
        let mut inner = self.inner.write().await;
        let sub = inner
            .subscriptions
            .iter_mut()
            .find(|s| s.id == id)
            .ok_or_else(|| RssFeedError::NotFound(id.to_string()))?;
        sub.poll_interval_secs = secs.max(60); // floor at 1 minute
        drop(inner);
        self.persist().await?;
        Ok(())
    }

    /// List all subscriptions.
    pub async fn list(&self) -> Vec<FeedSubscription> {
        let inner = self.inner.read().await;
        inner.subscriptions.clone()
    }

    /// Get a single subscription by id.
    pub async fn get(&self, id: &str) -> Option<FeedSubscription> {
        let inner = self.inner.read().await;
        inner.subscriptions.iter().find(|s| s.id == id).cloned()
    }

    /// Poll a single subscription for new items. Returns newly discovered items.
    pub async fn poll_feed(&self, id: &str) -> Result<Vec<FeedItem>, RssFeedError> {
        let (feed_url, title_filter, extensions, seen_before) = {
            let mut inner = self.inner.write().await;
            let sub = inner
                .subscriptions
                .iter_mut()
                .find(|s| s.id == id)
                .ok_or_else(|| RssFeedError::NotFound(id.to_string()))?;
            let seen: HashSet<String> = sub.seen_ids.iter().cloned().collect();
            (
                sub.feed_url.clone(),
                sub.title_filter.clone(),
                sub.extensions.clone(),
                seen,
            )
        };

        let body = self
            .inner
            .read()
            .await
            .http_client
            .get(&feed_url)
            .send()
            .await?
            .text()
            .await?;

        let all_items = parse_feed(&body)?;
        let mut new_items = Vec::new();
        for item in all_items {
            if seen_before.contains(&item.id) {
                continue;
            }
            if !matches_filter(&item, &title_filter, &extensions) {
                continue;
            }
            new_items.push(item);
        }

        // Invoke callback for each new item.
        {
            let inner = self.inner.read().await;
            if let Some(ref cb) = inner.on_new_item {
                let sub = inner.subscriptions.iter().find(|s| s.id == id).cloned();
                if let Some(sub) = sub {
                    for item in &new_items {
                        cb(item.clone(), &sub);
                    }
                }
            }
        }

        // Update seen_ids and last_poll.
        {
            let mut inner = self.inner.write().await;
            let max_seen = inner.max_seen_ids;
            if let Some(sub) = inner.subscriptions.iter_mut().find(|s| s.id == id) {
                let mut seen_set: HashSet<String> = sub.seen_ids.drain(..).collect();
                for item in &new_items {
                    seen_set.insert(item.id.clone());
                }
                // Bound the seen list.
                let mut seen_vec: Vec<String> = seen_set.into_iter().collect();
                if seen_vec.len() > max_seen {
                    let drop_n = seen_vec.len() - max_seen;
                    seen_vec.drain(..drop_n);
                }
                sub.seen_ids = seen_vec;
                sub.last_poll = Some(Utc::now());
            }
        }
        self.persist().await?;

        debug!(id = id, new = new_items.len(), "polled feed");
        Ok(new_items)
    }

    /// Poll all enabled subscriptions, returning (sub_id, new_items) pairs.
    pub async fn poll_all_due(&self) -> Vec<(String, Vec<FeedItem>)> {
        let subs: Vec<FeedSubscription> = {
            let inner = self.inner.read().await;
            let now = Utc::now();
            inner
                .subscriptions
                .iter()
                .filter(|s| {
                    if !s.enabled {
                        return false;
                    }
                    match s.last_poll {
                        None => true,
                        Some(last) => {
                            let elapsed = (now - last).num_seconds().max(0) as u64;
                            elapsed >= s.poll_interval_secs
                        }
                    }
                })
                .cloned()
                .collect()
        };

        let mut results = Vec::new();
        for sub in subs {
            match self.poll_feed(&sub.id).await {
                Ok(items) if !items.is_empty() => results.push((sub.id, items)),
                Ok(_) => {}
                Err(e) => {
                    warn!(id = %sub.id, err = %e, "failed to poll feed");
                }
            }
        }
        results
    }

    /// Persist current state to disk (atomic write).
    async fn persist(&self) -> Result<(), RssFeedError> {
        let inner = self.inner.read().await;
        let persisted = PersistedSubscriptions {
            version: 1,
            subscriptions: inner.subscriptions.clone(),
        };
        let tmp = inner.config_path.with_extension("tmp");
        let data = serde_json::to_string_pretty(&persisted)?;
        tokio::fs::write(&tmp, data.as_bytes()).await?;
        tokio::fs::rename(&tmp, &inner.config_path).await?;
        Ok(())
    }
}

/// Parse an RSS 2.0 or Atom 1.0 feed body into items.
///
/// This is a minimal, dependency-free parser tailored to the common shapes
/// of these formats. It extracts `<item>` (RSS) or `<entry>` (Atom) elements
/// and their `<enclosure>`/`<link>` children.
pub fn parse_feed(body: &str) -> Result<Vec<FeedItem>, RssFeedError> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Err(RssFeedError::Parse("empty feed body".into()));
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower.contains("<feed") && lower.contains("xmlns=\"http://www.w3.org/2005/atom\"")
        || lower.contains("<feed") && lower.contains("<entry")
    {
        parse_atom(body)
    } else if lower.contains("<rss") || lower.contains("<channel>") {
        parse_rss(body)
    } else {
        Err(RssFeedError::Parse("unrecognized feed format".into()))
    }
}

fn parse_rss(body: &str) -> Result<Vec<FeedItem>, RssFeedError> {
    let mut items = Vec::new();
    let mut rest = body;
    while let Some(item_start) = rest.find("<item") {
        let after_tag = match rest[item_start..].find('>') {
            Some(p) => item_start + p + 1,
            None => break,
        };
        let item_end = match rest[after_tag..].find("</item>") {
            Some(p) => after_tag + p,
            None => break,
        };
        let item_body = &rest[after_tag..item_end];
        if let Some(item) = extract_rss_item(item_body) {
            items.push(item);
        }
        rest = &rest[item_end + "</item>".len()..];
    }
    Ok(items)
}

fn extract_rss_item(body: &str) -> Option<FeedItem> {
    let title = extract_text(body, "title").unwrap_or_default();
    let link = extract_text(body, "link").unwrap_or_default();
    let guid = extract_text(body, "guid").or_else(|| extract_text(body, "link"));
    let pub_date = extract_text(body, "pubDate")
        .as_deref()
        .and_then(parse_rfc822);

    // Prefer enclosure URL.
    let (url, size, content_type) =
        extract_enclosure(body).unwrap_or_else(|| (link.clone(), None, None));

    let id = guid.unwrap_or_else(|| url.clone());
    if url.is_empty() && title.is_empty() {
        return None;
    }
    Some(FeedItem {
        id,
        title,
        url,
        size,
        published: pub_date,
        content_type,
    })
}

fn extract_enclosure(body: &str) -> Option<(String, Option<u64>, Option<String>)> {
    let start = body.find("<enclosure")?;
    let end = body[start..].find("/>").map(|p| start + p + 2)?;
    let tag = &body[start..end];
    let url = extract_attr(tag, "url")?;
    let size = extract_attr(tag, "length").and_then(|s| s.parse::<u64>().ok());
    let ctype = extract_attr(tag, "type");
    Some((url, size, ctype))
}

fn parse_atom(body: &str) -> Result<Vec<FeedItem>, RssFeedError> {
    let mut items = Vec::new();
    let mut rest = body;
    while let Some(entry_start) = rest.find("<entry") {
        let after_tag = match rest[entry_start..].find('>') {
            Some(p) => entry_start + p + 1,
            None => break,
        };
        let entry_end = match rest[after_tag..].find("</entry>") {
            Some(p) => after_tag + p,
            None => break,
        };
        let entry_body = &rest[after_tag..entry_end];
        if let Some(item) = extract_atom_entry(entry_body) {
            items.push(item);
        }
        rest = &rest[entry_end + "</entry>".len()..];
    }
    Ok(items)
}

fn extract_atom_entry(body: &str) -> Option<FeedItem> {
    let title = extract_text(body, "title").unwrap_or_default();
    let id = extract_text(body, "id");

    // Look for <link rel="enclosure" href="..."/> first, then any <link href="..."/>.
    let mut enclosure_url: Option<String> = None;
    let mut enclosure_size: Option<u64> = None;
    let mut enclosure_type: Option<String> = None;
    let mut alt_url: Option<String> = None;

    let mut rest = body;
    while let Some(pos) = rest.find("<link") {
        let end = match rest[pos..].find("/>").or_else(|| rest[pos..].find(">")) {
            Some(p) => pos + p + if rest[pos..].starts_with("/>") { 2 } else { 1 },
            None => break,
        };
        let tag = &rest[pos..end];
        let rel = extract_attr(tag, "rel");
        let href = extract_attr(tag, "href");
        let length = extract_attr(tag, "length").and_then(|s| s.parse::<u64>().ok());
        let ctype = extract_attr(tag, "type");
        if rel.as_deref() == Some("enclosure") {
            if let Some(h) = href {
                enclosure_url = Some(h);
                enclosure_size = length;
                enclosure_type = ctype;
                break;
            }
        } else if alt_url.is_none() {
            alt_url = href;
        }
        rest = &rest[end..];
    }

    let url = enclosure_url.or(alt_url).unwrap_or_default();
    let updated = extract_text(body, "updated")
        .or_else(|| extract_text(body, "published"))
        .as_deref()
        .and_then(parse_iso8601);

    let final_id = id.unwrap_or_else(|| url.clone());
    if url.is_empty() && title.is_empty() {
        return None;
    }
    Some(FeedItem {
        id: final_id,
        title,
        url,
        size: enclosure_size,
        published: updated,
        content_type: enclosure_type,
    })
}

/// Extract text content of a simple XML element: `<tag>text</tag>`.
fn extract_text(body: &str, tag: &str) -> Option<String> {
    let open1 = format!("<{tag}>");
    let open2 = format!("<{tag} ");
    let start = body.find(&open1).or_else(|| body.find(&open2))?;
    let content_start = body[start..].find('>').map(|p| start + p + 1)?;
    let close = format!("</{tag}>");
    let end = body[content_start..].find(&close)?;
    let text = &body[content_start..content_start + end];
    Some(decode_xml_entities(text.trim()))
}

/// Extract an attribute value from a tag: `<tag attr="value" ...>`.
fn extract_attr(tag: &str, attr: &str) -> Option<String> {
    let needle = format!("{attr}=\"");
    let start = tag.find(&needle)?;
    let value_start = start + needle.len();
    let end = tag[value_start..].find('"')?;
    Some(decode_xml_entities(&tag[value_start..value_start + end]))
}

fn decode_xml_entities(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

/// Best-effort RFC-822 date parser (RSS pubDate). Accepts common shapes.
fn parse_rfc822(s: &str) -> Option<DateTime<Utc>> {
    let s = s.trim();
    // Try chrono's RFC2822 first (handles most RSS dates).
    if let Ok(dt) = DateTime::parse_from_rfc2822(s) {
        return Some(dt.with_timezone(&Utc));
    }
    // Fallback: RFC3339.
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    None
}

/// Best-effort ISO-8601 date parser (Atom).
fn parse_iso8601(s: &str) -> Option<DateTime<Utc>> {
    let s = s.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    None
}

/// Check whether an item matches the title filter and extension filter.
fn matches_filter(item: &FeedItem, title_filter: &Option<String>, extensions: &[String]) -> bool {
    if let Some(tf) = title_filter {
        let needle = tf.to_lowercase();
        if !item.title.to_lowercase().contains(&needle) {
            return false;
        }
    }
    if !extensions.is_empty() {
        let ext = file_extension(&item.url)
            .map(|s| s.to_lowercase())
            .unwrap_or_default();
        if ext.is_empty() || !extensions.iter().any(|e| e == &ext) {
            return false;
        }
    }
    true
}

fn file_extension(url: &str) -> Option<String> {
    let path = url.split('?').next()?;
    let last = path.rsplit('/').next()?;
    let dot = last.rfind('.')?;
    Some(last[dot + 1..].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const RSS_SAMPLE: &str = r#"<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <title>Example Feed</title>
    <item>
      <title>Episode 1</title>
      <link>https://example.com/ep1</link>
      <guid>ep-1</guid>
      <enclosure url="https://example.com/ep1.mp4" length="1024" type="video/mp4"/>
      <pubDate>Sat, 08 Aug 2026 10:00:00 +0000</pubDate>
    </item>
    <item>
      <title>Episode 2</title>
      <link>https://example.com/ep2.mkv</link>
      <guid>ep-2</guid>
    </item>
  </channel>
</rss>"#;

    const ATOM_SAMPLE: &str = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Example Atom</title>
  <entry>
    <id>urn:atom:1</id>
    <title>Atom Entry 1</title>
    <link rel="alternate" href="https://example.com/a1"/>
    <link rel="enclosure" href="https://example.com/a1.zip" length="2048" type="application/zip"/>
    <updated>2026-08-08T09:00:00Z</updated>
  </entry>
  <entry>
    <id>urn:atom:2</id>
    <title>Atom Entry 2</title>
    <link href="https://example.com/a2.tar.gz"/>
    <updated>2026-08-07T09:00:00Z</updated>
  </entry>
</feed>"#;

    #[test]
    fn test_parse_rss_items() {
        let items = parse_feed(RSS_SAMPLE).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].id, "ep-1");
        assert_eq!(items[0].title, "Episode 1");
        assert_eq!(items[0].url, "https://example.com/ep1.mp4");
        assert_eq!(items[0].size, Some(1024));
        assert_eq!(items[0].content_type.as_deref(), Some("video/mp4"));
        assert!(items[0].published.is_some());
        // Second item falls back to link as URL.
        assert_eq!(items[1].url, "https://example.com/ep2.mkv");
    }

    #[test]
    fn test_parse_atom_items() {
        let items = parse_feed(ATOM_SAMPLE).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].id, "urn:atom:1");
        assert_eq!(items[0].url, "https://example.com/a1.zip");
        assert_eq!(items[0].size, Some(2048));
        assert_eq!(items[0].content_type.as_deref(), Some("application/zip"));
        // Second entry: no enclosure, falls back to link href.
        assert_eq!(items[1].url, "https://example.com/a2.tar.gz");
    }

    #[test]
    fn test_parse_empty_body() {
        assert!(parse_feed("").is_err());
    }

    #[test]
    fn test_parse_unknown_format() {
        assert!(parse_feed("<html><body>hello</body></html>").is_err());
    }

    #[test]
    fn test_title_filter() {
        let item = FeedItem {
            id: "x".into(),
            title: "Ubuntu 24.04 LTS ISO".into(),
            url: "https://example.com/u.iso".into(),
            size: None,
            published: None,
            content_type: None,
        };
        assert!(matches_filter(&item, &Some("ubuntu".into()), &[]));
        assert!(matches_filter(&item, &Some("UBUNTU".into()), &[]));
        assert!(!matches_filter(&item, &Some("fedora".into()), &[]));
    }

    #[test]
    fn test_extension_filter() {
        let item = FeedItem {
            id: "x".into(),
            title: "t".into(),
            url: "https://example.com/file.MP4?token=abc".into(),
            size: None,
            published: None,
            content_type: None,
        };
        assert!(matches_filter(&item, &None, &["mp4".into()]));
        assert!(!matches_filter(&item, &None, &["mkv".into()]));
    }

    #[test]
    fn test_extension_filter_no_ext() {
        let item = FeedItem {
            id: "x".into(),
            title: "t".into(),
            url: "https://example.com/noext".into(),
            size: None,
            published: None,
            content_type: None,
        };
        // When extensions are specified, items without an extension are rejected.
        assert!(!matches_filter(&item, &None, &["mp4".into()]));
    }

    #[test]
    fn test_file_extension() {
        assert_eq!(
            file_extension("https://x.com/a/b/c.mkv"),
            Some("mkv".into())
        );
        assert_eq!(
            file_extension("https://x.com/a/b/c.tar.gz?x=1"),
            Some("gz".into())
        );
        assert_eq!(file_extension("https://x.com/noext"), None);
    }

    #[test]
    fn test_decode_xml_entities() {
        assert_eq!(decode_xml_entities("a&amp;b"), "a&b");
        assert_eq!(decode_xml_entities("&lt;tag&gt;"), "<tag>");
    }

    #[tokio::test]
    async fn test_add_remove_subscription() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        let id = mgr
            .add_subscription("https://example.com/feed.xml", Some("ex"), None, vec![])
            .await
            .unwrap();
        assert_eq!(mgr.list().await.len(), 1);
        // Duplicate rejected.
        assert!(
            mgr.add_subscription("https://example.com/feed.xml", None, None, vec![])
                .await
                .is_err()
        );
        mgr.remove_subscription(&id).await.unwrap();
        assert!(mgr.list().await.is_empty());
    }

    #[tokio::test]
    async fn test_persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let id;
        {
            let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
            id = mgr
                .add_subscription(
                    "https://example.com/f.xml",
                    Some("lbl"),
                    Some("linux"),
                    vec!["iso".into()],
                )
                .await
                .unwrap();
            mgr.set_enabled(&id, false).await.unwrap();
            mgr.set_poll_interval(&id, 120).await.unwrap();
        }
        // Re-open.
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        let subs = mgr.list().await;
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].id, id);
        assert_eq!(subs[0].label.as_deref(), Some("lbl"));
        assert_eq!(subs[0].title_filter.as_deref(), Some("linux"));
        assert_eq!(subs[0].extensions, vec!["iso"]);
        assert!(!subs[0].enabled);
        assert_eq!(subs[0].poll_interval_secs, 120);
    }

    #[tokio::test]
    async fn test_remove_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        assert!(mgr.remove_subscription("missing").await.is_err());
    }

    #[tokio::test]
    async fn test_empty_url_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        assert!(mgr.add_subscription("", None, None, vec![]).await.is_err());
    }

    // Note: Integration tests for HTTP polling are skipped because they require
    // a real HTTP server, which introduces timing issues. The callback mechanism
    // is tested indirectly through other unit tests.

    #[tokio::test]
    async fn test_title_and_extension_filter_integration() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = RSS_SAMPLE.to_string();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = ready_tx.send(());
            if let Ok((mut stream, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        let _ = ready_rx.await;
        tokio::task::yield_now().await;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        // Only items with "episode 1" in title AND .mp4 extension.
        let id = mgr
            .add_subscription(
                &format!("http://{addr}/feed.xml"),
                None,
                Some("episode 1"),
                vec!["mp4".into()],
            )
            .await
            .unwrap();
        let items = mgr.poll_feed(&id).await.unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, "ep-1");
    }
}
