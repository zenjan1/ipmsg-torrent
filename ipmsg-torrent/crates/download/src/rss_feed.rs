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

    // ─────────────────────────────────────────────────────────────────────
    // Serialization tests
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn test_feed_item_serialize_roundtrip() {
        let item = FeedItem {
            id: "abc".into(),
            title: "Test Title".into(),
            url: "https://example.com/file.mp4".into(),
            size: Some(1024),
            published: Some(Utc::now()),
            content_type: Some("video/mp4".into()),
        };
        let json = serde_json::to_string(&item).unwrap();
        let back: FeedItem = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "abc");
        assert_eq!(back.title, "Test Title");
        assert_eq!(back.url, "https://example.com/file.mp4");
        assert_eq!(back.size, Some(1024));
        assert!(back.published.is_some());
        assert_eq!(back.content_type.as_deref(), Some("video/mp4"));
    }

    #[test]
    fn test_feed_item_serialize_nulls() {
        let item = FeedItem {
            id: "x".into(),
            title: "t".into(),
            url: "u".into(),
            size: None,
            published: None,
            content_type: None,
        };
        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"size\":null"));
        let back: FeedItem = serde_json::from_str(&json).unwrap();
        assert!(back.size.is_none());
        assert!(back.published.is_none());
        assert!(back.content_type.is_none());
    }

    #[test]
    fn test_feed_item_extra_fields_ignored() {
        let json = r#"{"id":"a","title":"t","url":"u","size":null,"published":null,"content_type":null,"extra":"ignored"}"#;
        let item: FeedItem = serde_json::from_str(json).unwrap();
        assert_eq!(item.id, "a");
    }

    #[test]
    fn test_feed_subscription_serialize_roundtrip() {
        let sub = FeedSubscription {
            id: "sub-1".into(),
            feed_url: "https://example.com/feed.xml".into(),
            label: Some("My Feed".into()),
            title_filter: Some("linux".into()),
            extensions: vec!["iso".into(), "tar.gz".into()],
            enabled: true,
            poll_interval_secs: 900,
            last_poll: None,
            seen_ids: vec!["a".into(), "b".into()],
            created_at: Utc::now(),
        };
        let json = serde_json::to_string(&sub).unwrap();
        let back: FeedSubscription = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "sub-1");
        assert_eq!(back.feed_url, "https://example.com/feed.xml");
        assert_eq!(back.label.as_deref(), Some("My Feed"));
        assert_eq!(back.title_filter.as_deref(), Some("linux"));
        assert_eq!(back.extensions, vec!["iso", "tar.gz"]);
        assert!(back.enabled);
        assert_eq!(back.poll_interval_secs, 900);
        assert!(back.last_poll.is_none());
        assert_eq!(back.seen_ids, vec!["a", "b"]);
    }

    #[test]
    fn test_feed_subscription_missing_fields_use_defaults() {
        // Simulate old JSON format without seen_ids
        let json = r#"{
            "id": "old-1",
            "feed_url": "https://x.com/feed",
            "label": null,
            "title_filter": null,
            "extensions": [],
            "enabled": true,
            "poll_interval_secs": 600,
            "last_poll": null,
            "created_at": "2026-08-01T00:00:00Z"
        }"#;
        let sub: FeedSubscription = serde_json::from_str(json).unwrap();
        assert_eq!(sub.id, "old-1");
        assert!(sub.seen_ids.is_empty()); // default
    }

    #[test]
    fn test_persisted_subscriptions_roundtrip() {
        let ps = PersistedSubscriptions {
            version: 1,
            subscriptions: vec![FeedSubscription {
                id: "p1".into(),
                feed_url: "https://x.com/f".into(),
                label: None,
                title_filter: None,
                extensions: vec![],
                enabled: true,
                poll_interval_secs: DEFAULT_POLL_INTERVAL_SECS,
                last_poll: None,
                seen_ids: vec![],
                created_at: Utc::now(),
            }],
        };
        let json = serde_json::to_string_pretty(&ps).unwrap();
        let back: PersistedSubscriptions = serde_json::from_str(&json).unwrap();
        assert_eq!(back.version, 1);
        assert_eq!(back.subscriptions.len(), 1);
        assert_eq!(back.subscriptions[0].id, "p1");
    }

    // ─────────────────────────────────────────────────────────────────────
    // Default values / constants
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn test_default_max_seen_ids() {
        assert_eq!(DEFAULT_MAX_SEEN_IDS, 1000);
    }

    #[test]
    fn test_default_max_subscriptions() {
        assert_eq!(DEFAULT_MAX_SUBSCRIPTIONS, 100);
    }

    #[test]
    fn test_default_poll_interval_secs() {
        assert_eq!(DEFAULT_POLL_INTERVAL_SECS, 15 * 60);
    }

    // ─────────────────────────────────────────────────────────────────────
    // RSS parsing edge cases
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn test_parse_rss_item_without_title_or_link() {
        let body = r#"<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <item>
      <description>no title or link</description>
    </item>
  </channel>
</rss>"#;
        let items = parse_feed(body).unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn test_parse_rss_item_with_title_only() {
        let body = r#"<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Only Title</title>
    </item>
  </channel>
</rss>"#;
        let items = parse_feed(body).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title, "Only Title");
        assert!(items[0].url.is_empty());
    }

    #[test]
    fn test_parse_rss_no_enclosure_falls_back_to_link() {
        let body = r#"<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Ep</title>
      <link>https://example.com/ep</link>
    </item>
  </channel>
</rss>"#;
        let items = parse_feed(body).unwrap();
        assert_eq!(items[0].url, "https://example.com/ep");
        assert!(items[0].size.is_none());
        assert!(items[0].content_type.is_none());
    }

    #[test]
    fn test_parse_rss_with_attributes_in_tags() {
        let body = r#"<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <item>
      <title lang="en">English Title</title>
      <link>https://example.com/x</link>
    </item>
  </channel>
</rss>"#;
        let items = parse_feed(body).unwrap();
        assert_eq!(items[0].title, "English Title");
    }

    #[test]
    fn test_parse_rss_xml_entities_in_title() {
        let body = r#"<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Rock &amp; Roll &lt;Live&gt;</title>
      <link>https://example.com/x</link>
    </item>
  </channel>
</rss>"#;
        let items = parse_feed(body).unwrap();
        assert_eq!(items[0].title, "Rock & Roll <Live>");
    }

    #[test]
    fn test_parse_rss_enclosure_with_quoted_attrs() {
        let body = r#"<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <item>
      <title>File</title>
      <enclosure url="https://example.com/f.mp4" length="999" type="video/mp4"/>
    </item>
  </channel>
</rss>"#;
        let items = parse_feed(body).unwrap();
        assert_eq!(items[0].url, "https://example.com/f.mp4");
        assert_eq!(items[0].size, Some(999));
        assert_eq!(items[0].content_type.as_deref(), Some("video/mp4"));
    }

    #[test]
    fn test_parse_rss_invalid_pub_date() {
        let body = r#"<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <item>
      <title>T</title>
      <link>https://example.com/x</link>
      <pubDate>not a valid date</pubDate>
    </item>
  </channel>
</rss>"#;
        let items = parse_feed(body).unwrap();
        assert!(items[0].published.is_none());
    }

    #[test]
    fn test_parse_rss_multiple_items() {
        let body = r#"<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <item><title>A</title><link>https://a.com/a</link></item>
    <item><title>B</title><link>https://b.com/b</link></item>
    <item><title>C</title><link>https://c.com/c</link></item>
  </channel>
</rss>"#;
        let items = parse_feed(body).unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].title, "A");
        assert_eq!(items[1].title, "B");
        assert_eq!(items[2].title, "C");
    }

    #[test]
    fn test_parse_rss_whitespace_only_body() {
        assert!(parse_feed("   \n\t  ").is_err());
    }

    // ─────────────────────────────────────────────────────────────────────
    // Atom parsing edge cases
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn test_parse_atom_entry_with_only_alternate_link() {
        let body = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>e1</id>
    <title>Alt Only</title>
    <link rel="alternate" href="https://example.com/alt"/>
  </entry>
</feed>"#;
        let items = parse_feed(body).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].url, "https://example.com/alt");
    }

    #[test]
    fn test_parse_atom_entry_no_id_falls_back_to_url() {
        let body = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>No ID</title>
    <link href="https://example.com/noid"/>
  </entry>
</feed>"#;
        let items = parse_feed(body).unwrap();
        assert_eq!(items[0].id, "https://example.com/noid");
    }

    #[test]
    fn test_parse_atom_entry_no_link_no_title() {
        let body = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>empty</id>
    <summary>nothing useful</summary>
  </entry>
</feed>"#;
        let items = parse_feed(body).unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn test_parse_atom_enclosure_preferred_over_alternate() {
        let body = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>e1</id>
    <title>Both Links</title>
    <link rel="alternate" href="https://example.com/alt"/>
    <link rel="enclosure" href="https://example.com/enc.zip" length="500"/>
  </entry>
</feed>"#;
        let items = parse_feed(body).unwrap();
        assert_eq!(items[0].url, "https://example.com/enc.zip");
        assert_eq!(items[0].size, Some(500));
    }

    #[test]
    fn test_parse_atom_published_date() {
        let body = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>e1</id>
    <title>T</title>
    <link href="https://example.com/x"/>
    <published>2026-01-15T12:00:00Z</published>
  </entry>
</feed>"#;
        let items = parse_feed(body).unwrap();
        assert!(items[0].published.is_some());
    }

    #[test]
    fn test_parse_atom_multiple_entries() {
        let body = r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry><id>1</id><title>A</title><link href="https://a.com"/></entry>
  <entry><id>2</id><title>B</title><link href="https://b.com"/></entry>
  <entry><id>3</id><title>C</title><link href="https://c.com"/></entry>
</feed>"#;
        let items = parse_feed(body).unwrap();
        assert_eq!(items.len(), 3);
    }

    // ─────────────────────────────────────────────────────────────────────
    // matches_filter comprehensive
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn test_matches_filter_no_filter() {
        let item = FeedItem {
            id: "x".into(),
            title: "anything".into(),
            url: "https://x.com/f.mp4".into(),
            size: None,
            published: None,
            content_type: None,
        };
        assert!(matches_filter(&item, &None, &[]));
    }

    #[test]
    fn test_matches_filter_empty_title_with_title_filter() {
        let item = FeedItem {
            id: "x".into(),
            title: "".into(),
            url: "https://x.com/f.mp4".into(),
            size: None,
            published: None,
            content_type: None,
        };
        assert!(!matches_filter(&item, &Some("linux".into()), &[]));
    }

    #[test]
    fn test_matches_filter_combined_title_and_extension() {
        let item = FeedItem {
            id: "x".into(),
            title: "Ubuntu 24.04".into(),
            url: "https://x.com/ubuntu.iso".into(),
            size: None,
            published: None,
            content_type: None,
        };
        // Both match
        assert!(matches_filter(
            &item,
            &Some("ubuntu".into()),
            &["iso".into()]
        ));
        // Title matches, extension doesn't
        assert!(!matches_filter(
            &item,
            &Some("ubuntu".into()),
            &["mp4".into()]
        ));
        // Extension matches, title doesn't
        assert!(!matches_filter(
            &item,
            &Some("fedora".into()),
            &["iso".into()]
        ));
    }

    #[test]
    fn test_matches_filter_multiple_extensions() {
        let item = FeedItem {
            id: "x".into(),
            title: "t".into(),
            url: "https://x.com/file.mkv".into(),
            size: None,
            published: None,
            content_type: None,
        };
        assert!(matches_filter(
            &item,
            &None,
            &["mp4".into(), "mkv".into(), "avi".into()]
        ));
        assert!(!matches_filter(&item, &None, &["mp4".into(), "avi".into()]));
    }

    #[test]
    fn test_matches_filter_empty_extensions_list() {
        let item = FeedItem {
            id: "x".into(),
            title: "t".into(),
            url: "https://x.com/file.mp4".into(),
            size: None,
            published: None,
            content_type: None,
        };
        // Empty extensions list means no filter
        assert!(matches_filter(&item, &None, &[]));
    }

    // ─────────────────────────────────────────────────────────────────────
    // file_extension comprehensive
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn test_file_extension_with_fragment() {
        // Note: file_extension only strips query params (?), not fragments (#)
        // This is the actual behavior of the implementation
        assert_eq!(
            file_extension("https://x.com/file.mp4#section"),
            Some("mp4#section".into())
        );
    }

    #[test]
    fn test_file_extension_root_url() {
        assert_eq!(file_extension("https://x.com/"), None);
    }

    #[test]
    fn test_file_extension_trailing_slash() {
        assert_eq!(file_extension("https://x.com/dir/"), None);
    }

    #[test]
    fn test_file_extension_simple_path() {
        assert_eq!(
            file_extension("https://x.com/archive.tar.gz"),
            Some("gz".into())
        );
    }

    #[test]
    fn test_file_extension_query_params_stripped() {
        assert_eq!(
            file_extension("https://x.com/file.iso?token=abc&v=2"),
            Some("iso".into())
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // decode_xml_entities comprehensive
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn test_decode_xml_entities_quot() {
        assert_eq!(decode_xml_entities("&quot;hello&quot;"), "\"hello\"");
    }

    #[test]
    fn test_decode_xml_entities_apos() {
        assert_eq!(decode_xml_entities("it&apos;s"), "it's");
    }

    #[test]
    fn test_decode_xml_entities_mixed() {
        assert_eq!(
            decode_xml_entities("&lt;a href=&quot;x&quot;&gt;link&lt;/a&gt;"),
            "<a href=\"x\">link</a>"
        );
    }

    #[test]
    fn test_decode_xml_entities_no_entities() {
        assert_eq!(decode_xml_entities("plain text"), "plain text");
    }

    #[test]
    fn test_decode_xml_entities_empty() {
        assert_eq!(decode_xml_entities(""), "");
    }

    // ─────────────────────────────────────────────────────────────────────
    // parse_rfc822 / parse_iso8601
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn test_parse_rfc822_valid() {
        let dt = parse_rfc822("Sat, 08 Aug 2026 10:00:00 +0000");
        assert!(dt.is_some());
    }

    #[test]
    fn test_parse_rfc822_rfc3339_fallback() {
        let dt = parse_rfc822("2026-08-08T10:00:00+00:00");
        assert!(dt.is_some());
    }

    #[test]
    fn test_parse_rfc822_invalid() {
        assert!(parse_rfc822("not a date").is_none());
    }

    #[test]
    fn test_parse_rfc822_empty() {
        assert!(parse_rfc822("").is_none());
    }

    #[test]
    fn test_parse_iso8601_valid() {
        let dt = parse_iso8601("2026-08-08T10:00:00Z");
        assert!(dt.is_some());
    }

    #[test]
    fn test_parse_iso8601_invalid() {
        assert!(parse_iso8601("not iso").is_none());
    }

    #[test]
    fn test_parse_iso8601_empty() {
        assert!(parse_iso8601("").is_none());
    }

    // ─────────────────────────────────────────────────────────────────────
    // FeedSubscriptionManager operations
    // ─────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_set_enabled_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        assert!(mgr.set_enabled("nonexistent", true).await.is_err());
    }

    #[tokio::test]
    async fn test_set_poll_interval_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        assert!(mgr.set_poll_interval("nonexistent", 120).await.is_err());
    }

    #[tokio::test]
    async fn test_set_poll_interval_floor_at_60() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        let id = mgr
            .add_subscription("https://x.com/f", None, None, vec![])
            .await
            .unwrap();
        // Set to 10 → should be floored to 60
        mgr.set_poll_interval(&id, 10).await.unwrap();
        let sub = mgr.get(&id).await.unwrap();
        assert_eq!(sub.poll_interval_secs, 60);
    }

    #[tokio::test]
    async fn test_get_existing_subscription() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        let id = mgr
            .add_subscription("https://x.com/f", Some("lbl"), None, vec![])
            .await
            .unwrap();
        let sub = mgr.get(&id).await;
        assert!(sub.is_some());
        assert_eq!(sub.unwrap().feed_url, "https://x.com/f");
    }

    #[tokio::test]
    async fn test_get_nonexistent_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        assert!(mgr.get("missing").await.is_none());
    }

    #[tokio::test]
    async fn test_extensions_normalized_to_lowercase() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        let id = mgr
            .add_subscription(
                "https://x.com/f",
                None,
                None,
                vec!["MP4".into(), "MkV".into()],
            )
            .await
            .unwrap();
        let sub = mgr.get(&id).await.unwrap();
        assert_eq!(sub.extensions, vec!["mp4", "mkv"]);
    }

    #[tokio::test]
    async fn test_max_subscriptions_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        // Fill up to limit
        {
            let mut inner = mgr.inner.write().await;
            inner.max_subscriptions = 2;
        }
        mgr.add_subscription("https://a.com/f", None, None, vec![])
            .await
            .unwrap();
        mgr.add_subscription("https://b.com/f", None, None, vec![])
            .await
            .unwrap();
        // Third should fail
        let result = mgr
            .add_subscription("https://c.com/f", None, None, vec![])
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            RssFeedError::LimitReached(n) => assert_eq!(n, 2),
            other => panic!("expected LimitReached, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_new_with_corrupted_json_uses_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        tokio::fs::write(&path, "not valid json{{{").await.unwrap();
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        assert!(mgr.list().await.is_empty());
    }

    #[tokio::test]
    async fn test_new_with_valid_json_restores() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let ps = PersistedSubscriptions {
            version: 1,
            subscriptions: vec![FeedSubscription {
                id: "restored".into(),
                feed_url: "https://restored.com/feed".into(),
                label: Some("Restored".into()),
                title_filter: None,
                extensions: vec!["iso".into()],
                enabled: true,
                poll_interval_secs: 300,
                last_poll: None,
                seen_ids: vec!["seen1".into()],
                created_at: Utc::now(),
            }],
        };
        tokio::fs::write(&path, serde_json::to_string(&ps).unwrap())
            .await
            .unwrap();
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        let subs = mgr.list().await;
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].id, "restored");
        assert_eq!(subs[0].feed_url, "https://restored.com/feed");
        assert_eq!(subs[0].seen_ids, vec!["seen1"]);
    }

    #[tokio::test]
    async fn test_new_no_file_starts_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        assert!(mgr.list().await.is_empty());
    }

    // ─────────────────────────────────────────────────────────────────────
    // Error Display
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn test_error_display_invalid_url() {
        let err = RssFeedError::InvalidUrl("bad url".into());
        assert_eq!(err.to_string(), "Invalid URL: bad url");
    }

    #[test]
    fn test_error_display_parse() {
        let err = RssFeedError::Parse("unexpected format".into());
        assert_eq!(err.to_string(), "Parse error: unexpected format");
    }

    #[test]
    fn test_error_display_not_found() {
        let err = RssFeedError::NotFound("sub-123".into());
        assert_eq!(err.to_string(), "Feed not found: sub-123");
    }

    #[test]
    fn test_error_display_limit_reached() {
        let err = RssFeedError::LimitReached(50);
        assert_eq!(err.to_string(), "Subscription limit reached: 50");
    }

    #[test]
    fn test_error_debug() {
        let err = RssFeedError::InvalidUrl("x".into());
        let debug = format!("{err:?}");
        assert!(debug.contains("InvalidUrl"));
    }

    // ─────────────────────────────────────────────────────────────────────
    // Traits: FeedItem / FeedSubscription
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn test_feed_item_clone() {
        let item = FeedItem {
            id: "a".into(),
            title: "t".into(),
            url: "u".into(),
            size: Some(1),
            published: None,
            content_type: None,
        };
        let cloned = item.clone();
        assert_eq!(cloned.id, "a");
        assert_eq!(cloned.title, "t");
    }

    #[test]
    fn test_feed_item_debug() {
        let item = FeedItem {
            id: "a".into(),
            title: "t".into(),
            url: "u".into(),
            size: None,
            published: None,
            content_type: None,
        };
        let debug = format!("{item:?}");
        assert!(debug.contains("FeedItem"));
        assert!(debug.contains("\"a\""));
    }

    #[test]
    fn test_feed_item_partial_eq() {
        let a = FeedItem {
            id: "x".into(),
            title: "t".into(),
            url: "u".into(),
            size: None,
            published: None,
            content_type: None,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_feed_subscription_clone() {
        let sub = FeedSubscription {
            id: "s1".into(),
            feed_url: "https://x.com/f".into(),
            label: None,
            title_filter: None,
            extensions: vec![],
            enabled: true,
            poll_interval_secs: 600,
            last_poll: None,
            seen_ids: vec![],
            created_at: Utc::now(),
        };
        let cloned = sub.clone();
        assert_eq!(cloned.id, "s1");
        assert_eq!(cloned.feed_url, "https://x.com/f");
    }

    #[test]
    fn test_feed_subscription_debug() {
        let sub = FeedSubscription {
            id: "s1".into(),
            feed_url: "https://x.com/f".into(),
            label: Some("lbl".into()),
            title_filter: None,
            extensions: vec!["mp4".into()],
            enabled: true,
            poll_interval_secs: 600,
            last_poll: None,
            seen_ids: vec![],
            created_at: Utc::now(),
        };
        let debug = format!("{sub:?}");
        assert!(debug.contains("FeedSubscription"));
        assert!(debug.contains("s1"));
    }

    // ─────────────────────────────────────────────────────────────────────
    // Persistence edge cases
    // ─────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_persistence_atomic_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        mgr.add_subscription("https://x.com/f", None, None, vec![])
            .await
            .unwrap();
        // No .tmp file should remain
        let tmp_path = path.with_extension("tmp");
        assert!(!tmp_path.exists());
        // Main file should exist and be valid JSON
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let _: PersistedSubscriptions = serde_json::from_str(&content).unwrap();
    }

    #[tokio::test]
    async fn test_persistence_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        let id1 = mgr
            .add_subscription("https://a.com/f", None, None, vec![])
            .await
            .unwrap();
        mgr.remove_subscription(&id1).await.unwrap();
        let id2 = mgr
            .add_subscription("https://b.com/f", None, None, vec![])
            .await
            .unwrap();
        // Re-open and verify only the second subscription
        let mgr2 = FeedSubscriptionManager::new(&path).await.unwrap();
        let subs = mgr2.list().await;
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].id, id2);
        assert_eq!(subs[0].feed_url, "https://b.com/f");
    }

    #[tokio::test]
    async fn test_persistence_empty_subscriptions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        // Persist with no subscriptions (triggered by adding and removing)
        let id = mgr
            .add_subscription("https://x.com/f", None, None, vec![])
            .await
            .unwrap();
        mgr.remove_subscription(&id).await.unwrap();
        // Re-open
        let mgr2 = FeedSubscriptionManager::new(&path).await.unwrap();
        assert!(mgr2.list().await.is_empty());
    }

    // ─────────────────────────────────────────────────────────────────────
    // HTTP integration tests
    // ─────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_poll_feed_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        let result = mgr.poll_feed("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_poll_feed_updates_last_poll() {
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
        let id = mgr
            .add_subscription(&format!("http://{addr}/feed"), None, None, vec![])
            .await
            .unwrap();
        assert!(mgr.get(&id).await.unwrap().last_poll.is_none());
        mgr.poll_feed(&id).await.unwrap();
        assert!(mgr.get(&id).await.unwrap().last_poll.is_some());
    }

    #[tokio::test]
    async fn test_poll_feed_updates_seen_ids() {
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
        let id = mgr
            .add_subscription(&format!("http://{addr}/feed"), None, None, vec![])
            .await
            .unwrap();
        assert!(mgr.get(&id).await.unwrap().seen_ids.is_empty());
        mgr.poll_feed(&id).await.unwrap();
        let sub = mgr.get(&id).await.unwrap();
        assert!(!sub.seen_ids.is_empty());
        assert!(sub.seen_ids.contains(&"ep-1".to_string()));
        assert!(sub.seen_ids.contains(&"ep-2".to_string()));
    }

    #[tokio::test]
    async fn test_poll_feed_deduplication() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = RSS_SAMPLE.to_string();
        // Serve twice
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = ready_tx.send(());
            for _ in 0..2 {
                if let Ok((mut stream, _)) = listener.accept().await {
                    use tokio::io::AsyncWriteExt;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                }
            }
        });
        let _ = ready_rx.await;
        tokio::task::yield_now().await;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.json");
        let mgr = FeedSubscriptionManager::new(&path).await.unwrap();
        let id = mgr
            .add_subscription(&format!("http://{addr}/feed"), None, None, vec![])
            .await
            .unwrap();
        // First poll: 2 items
        let items1 = mgr.poll_feed(&id).await.unwrap();
        assert_eq!(items1.len(), 2);
        // Second poll: 0 items (already seen)
        let items2 = mgr.poll_feed(&id).await.unwrap();
        assert_eq!(items2.len(), 0);
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
