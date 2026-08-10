//! URL Rewrite Rules (Phase 69)
//!
//! Pattern-based URL transformation rules applied before creating download tasks.
//! Useful for converting share links to direct download URLs, handling URL shorteners,
//! or normalizing URLs from various sources.
//!
//! # Example Rules
//! - Google Drive: `https://drive.google.com/file/d/{id}/view` → `https://drive.google.com/uc?export=download&id={id}`
//! - Custom domain rewrite: `https://old-site.com/*` → `https://new-site.com/*`
//! - Protocol upgrade: `http://*` → `https://*`

use serde::{Deserialize, Serialize};
use std::path::Path;

/// Error type for URL rewrite persistence operations
#[derive(Debug)]
pub enum UrlRewritePersistenceError {
    Io(std::io::Error),
    Serde(serde_json::Error),
}

impl std::fmt::Display for UrlRewritePersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Serde(e) => write!(f, "serialization error: {e}"),
        }
    }
}

impl std::error::Error for UrlRewritePersistenceError {}

impl From<std::io::Error> for UrlRewritePersistenceError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for UrlRewritePersistenceError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e)
    }
}

/// Pattern type for matching URLs
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RewritePattern {
    /// Wildcard pattern using `*` to match any sequence of characters
    /// Example: `https://drive.google.com/file/d/*/view`
    Wildcard(String),
    /// Regular expression pattern (uses regex-lite for compatibility)
    /// Capture groups can be referenced in the replacement template
    Regex(String),
    /// Exact string match (the entire URL must match)
    Exact(String),
    /// Prefix match (URL starts with the given string)
    Prefix(String),
}

impl RewritePattern {
    /// Test if a URL matches this pattern
    pub fn matches(&self, url: &str) -> bool {
        match self {
            RewritePattern::Wildcard(pat) => wildcard_match(pat, url),
            RewritePattern::Regex(pat) => regex_lite::Regex::new(pat)
                .map(|re| re.is_match(url))
                .unwrap_or(false),
            RewritePattern::Exact(exact) => url == exact,
            RewritePattern::Prefix(prefix) => url.starts_with(prefix),
        }
    }
}

/// Simple wildcard matching where `*` matches any sequence of characters
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();

    if parts.len() == 1 {
        // No wildcard, exact match
        return text == pattern;
    }

    let mut pos = 0;

    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        if i == 0 {
            // First segment must match at the start
            if !text.starts_with(part) {
                return false;
            }
            pos = part.len();
        } else if i == parts.len() - 1 {
            // Last segment must match at the end
            if !text[pos..].ends_with(part) {
                return false;
            }
            // Ensure the last segment doesn't overlap with what we've consumed
            let remaining = &text[pos..];
            if let Some(found_pos) =
                remaining[remaining.len().saturating_sub(part.len())..].find(part)
            {
                let abs_pos = remaining.len().saturating_sub(part.len()) + found_pos;
                if abs_pos + part.len() > remaining.len() {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            // Middle segments can match anywhere after current position
            if let Some(found) = text[pos..].find(part) {
                pos += found + part.len();
            } else {
                return false;
            }
        }
    }

    // If pattern ends with *, any trailing text is fine
    if pattern.ends_with('*') {
        return true;
    }

    // If pattern doesn't end with *, text must end exactly where last segment ends
    let last_part = parts.last().unwrap();
    text.ends_with(last_part)
}

/// Extract captured groups from a pattern match
/// Returns Vec where [0] = full URL match, [1..] = capture groups
fn extract_captures(pattern: &RewritePattern, url: &str) -> Option<Vec<String>> {
    match pattern {
        RewritePattern::Regex(pat) => {
            let re = regex_lite::Regex::new(pat).ok()?;
            let caps = re.captures(url)?;
            let mut groups = Vec::new();
            for i in 0..caps.len() {
                groups.push(
                    caps.get(i)
                        .map(|m| m.as_str().to_string())
                        .unwrap_or_default(),
                );
            }
            Some(groups)
        }
        RewritePattern::Wildcard(pat) => {
            // Extract wildcard captures (text matched by *)
            let parts: Vec<&str> = pat.split('*').collect();
            if parts.len() == 1 {
                // No wildcard, no captures - just full match
                return if url == pat {
                    Some(vec![url.to_string()])
                } else {
                    None
                };
            }

            let mut captures = vec![url.to_string()]; // [0] = full match
            let mut pos = 0;

            for (i, part) in parts.iter().enumerate() {
                if i == 0 {
                    if !part.is_empty() && !url.starts_with(part) {
                        return None;
                    }
                    pos = part.len();
                } else if i == parts.len() - 1 {
                    // Last segment
                    if !part.is_empty() {
                        if !url[pos..].ends_with(part) {
                            return None;
                        }
                        let end = url.len() - part.len();
                        if i > 1 && parts[i - 1].is_empty() {
                            captures.push(String::new());
                        } else {
                            captures.push(url[pos..end].to_string());
                        }
                        pos = end + part.len();
                    } else {
                        // Trailing *, capture is everything remaining
                        captures.push(url[pos..].to_string());
                    }
                } else if part.is_empty() {
                    // Empty part between two *s
                } else {
                    let found = url[pos..].find(part)?;
                    captures.push(url[pos..pos + found].to_string());
                    pos += found + part.len();
                }
            }

            Some(captures)
        }
        RewritePattern::Prefix(prefix) => {
            if url.starts_with(prefix) {
                let rest = &url[prefix.len()..];
                Some(vec![url.to_string(), rest.to_string()])
            } else {
                None
            }
        }
        RewritePattern::Exact(exact) => {
            if url == exact {
                Some(vec![url.to_string()])
            } else {
                None
            }
        }
    }
}

/// Apply a replacement template using captured groups
/// Supports `$0` for full match, `$1`, `$2`, etc. for capture groups
/// `captures` is [full_match, group1, group2, ...] from regex-style extraction
fn apply_template(template: &str, url: &str, captures: &[String]) -> String {
    let mut result = template.to_string();

    // Replace $0 with full URL
    result = result.replace("$0", url);

    // Replace $1, $2, etc. with capture groups
    // captures[0] = full match, captures[1] = first group, captures[2] = second group, etc.
    // So $N maps to captures[N]
    for i in 1..captures.len() {
        let placeholder = format!("${}", i);
        result = result.replace(&placeholder, &captures[i]);
    }

    result
}

/// A single URL rewrite rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlRewriteRule {
    /// Unique identifier
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Pattern to match URLs against
    pub pattern: RewritePattern,
    /// Replacement template (supports $1, $2, ... for captures)
    pub replacement: String,
    /// Whether this rule is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Priority (higher = checked first, default 0)
    #[serde(default)]
    pub priority: i32,
    /// Optional description
    #[serde(default)]
    pub description: Option<String>,
    /// Number of times this rule has been applied
    #[serde(default)]
    pub apply_count: u64,
}

fn default_true() -> bool {
    true
}

impl UrlRewriteRule {
    /// Try to rewrite a URL using this rule.
    /// Returns `Some(rewritten_url)` if the pattern matches, `None` otherwise.
    pub fn try_rewrite(&self, url: &str) -> Option<String> {
        if !self.enabled {
            return None;
        }

        if self.pattern.matches(url) {
            let captures = extract_captures(&self.pattern, url)?;
            let result = apply_template(&self.replacement, url, &captures);
            Some(result)
        } else {
            None
        }
    }
}

/// Manager for URL rewrite rules
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UrlRewriteManager {
    /// List of rewrite rules (sorted by priority descending)
    pub rules: Vec<UrlRewriteRule>,
    /// Whether URL rewriting is globally enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl UrlRewriteManager {
    /// Create a new empty manager
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            enabled: true,
        }
    }

    /// Add a rewrite rule
    pub fn add_rule(&mut self, rule: UrlRewriteRule) {
        self.rules.push(rule);
        self.sort_rules();
    }

    /// Remove a rewrite rule by ID
    pub fn remove_rule(&mut self, id: &str) -> bool {
        let before = self.rules.len();
        self.rules.retain(|r| r.id != id);
        self.rules.len() < before
    }

    /// Get a rule by ID
    pub fn get_rule(&self, id: &str) -> Option<&UrlRewriteRule> {
        self.rules.iter().find(|r| r.id == id)
    }

    /// Get a mutable reference to a rule by ID
    pub fn get_rule_mut(&mut self, id: &str) -> Option<&mut UrlRewriteRule> {
        self.rules.iter_mut().find(|r| r.id == id)
    }

    /// List all rules
    pub fn list_rules(&self) -> &[UrlRewriteRule] {
        &self.rules
    }

    /// Apply rewrite rules to a URL.
    /// Returns the rewritten URL if any rule matches (first match wins, by priority).
    /// Also increments the apply_count of the matched rule.
    pub fn rewrite_url(&mut self, url: &str) -> String {
        if !self.enabled {
            return url.to_string();
        }

        for rule in &mut self.rules {
            if let Some(rewritten) = rule.try_rewrite(url) {
                rule.apply_count += 1;
                return rewritten;
            }
        }

        url.to_string()
    }

    /// Apply rewrite rules to a URL without modifying apply counts (read-only).
    pub fn preview_rewrite(&self, url: &str) -> Option<(String, String)> {
        if !self.enabled {
            return None;
        }

        for rule in &self.rules {
            if let Some(rewritten) = rule.try_rewrite(url) {
                return Some((rewritten, rule.name.clone()));
            }
        }

        None
    }

    /// Sort rules by priority (descending)
    fn sort_rules(&mut self) {
        self.rules.sort_by_key(|r| std::cmp::Reverse(r.priority));
    }

    /// Get summary of all rules
    pub fn summary(&self) -> UrlRewriteSummary {
        let total = self.rules.len();
        let enabled = self.rules.iter().filter(|r| r.enabled).count();
        let total_applies: u64 = self.rules.iter().map(|r| r.apply_count).sum();

        UrlRewriteSummary {
            global_enabled: self.enabled,
            total_rules: total,
            enabled_rules: enabled,
            total_applies,
            rules: self
                .rules
                .iter()
                .map(|r| UrlRewriteRuleInfo {
                    id: r.id.clone(),
                    name: r.name.clone(),
                    pattern: format!("{:?}", r.pattern),
                    replacement: r.replacement.clone(),
                    enabled: r.enabled,
                    priority: r.priority,
                    apply_count: r.apply_count,
                })
                .collect(),
        }
    }
}

/// Summary of URL rewrite configuration and statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlRewriteSummary {
    pub global_enabled: bool,
    pub total_rules: usize,
    pub enabled_rules: usize,
    pub total_applies: u64,
    pub rules: Vec<UrlRewriteRuleInfo>,
}

/// Info about a single rewrite rule (for display)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlRewriteRuleInfo {
    pub id: String,
    pub name: String,
    pub pattern: String,
    pub replacement: String,
    pub enabled: bool,
    pub priority: i32,
    pub apply_count: u64,
}

impl UrlRewriteSummary {
    /// Format summary for display
    pub fn format_report(&self) -> String {
        let mut lines = Vec::new();
        lines.push("═══ URL Rewrite Rules ═══".to_string());
        lines.push(format!(
            "Global: {}",
            if self.global_enabled {
                "✅ Enabled"
            } else {
                "❌ Disabled"
            }
        ));
        lines.push(format!(
            "Rules: {} total, {} enabled",
            self.total_rules, self.enabled_rules
        ));
        lines.push(format!("Total applies: {}", self.total_applies));
        lines.push(String::new());

        if self.rules.is_empty() {
            lines.push("  (no rules configured)".to_string());
        } else {
            for rule in &self.rules {
                let status = if rule.enabled { "✅" } else { "❌" };
                lines.push(format!(
                    "  {} [{}] {} (priority: {}, applies: {})",
                    status, rule.id, rule.name, rule.priority, rule.apply_count
                ));
                lines.push(format!("    Pattern: {}", rule.pattern));
                lines.push(format!("    Replace: {}", rule.replacement));
            }
        }

        lines.join("\n")
    }
}

/// Persistence functions
///
/// Save URL rewrite rules to disk (atomic write)
pub fn save_url_rewrite_manager(
    manager: &UrlRewriteManager,
    data_dir: &Path,
) -> Result<(), UrlRewritePersistenceError> {
    let path = data_dir.join("url_rewrite_rules.json");
    let json = serde_json::to_string_pretty(manager)?;
    let temp_path = data_dir.join("url_rewrite_rules.json.tmp");
    std::fs::write(&temp_path, &json)?;
    std::fs::rename(temp_path, path)?;
    Ok(())
}

/// Load URL rewrite rules from disk
pub fn load_url_rewrite_manager(data_dir: &Path) -> Option<UrlRewriteManager> {
    let path = data_dir.join("url_rewrite_rules.json");
    let json = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&json).ok()
}

/// Parse a pattern string into a RewritePattern
/// Prefix with `regex:` for regex, `exact:` for exact, `prefix:` for prefix,
/// otherwise defaults to wildcard
pub fn parse_pattern(s: &str) -> RewritePattern {
    if let Some(rest) = s.strip_prefix("regex:") {
        RewritePattern::Regex(rest.to_string())
    } else if let Some(rest) = s.strip_prefix("exact:") {
        RewritePattern::Exact(rest.to_string())
    } else if let Some(rest) = s.strip_prefix("prefix:") {
        RewritePattern::Prefix(rest.to_string())
    } else {
        RewritePattern::Wildcard(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wildcard_match_exact() {
        assert!(wildcard_match(
            "https://example.com/file.zip",
            "https://example.com/file.zip"
        ));
        assert!(!wildcard_match(
            "https://example.com/file.zip",
            "https://example.com/other.zip"
        ));
    }

    #[test]
    fn test_wildcard_match_star() {
        assert!(wildcard_match(
            "https://example.com/*",
            "https://example.com/anything"
        ));
        assert!(wildcard_match(
            "https://example.com/*/view",
            "https://example.com/abc123/view"
        ));
        assert!(!wildcard_match(
            "https://example.com/*/view",
            "https://example.com/abc123/edit"
        ));
    }

    #[test]
    fn test_wildcard_multiple_stars() {
        assert!(wildcard_match(
            "https://*.example.com/*/download",
            "https://cdn.example.com/files/download"
        ));
        assert!(!wildcard_match(
            "https://*.example.com/*/download",
            "https://cdn.other.com/files/download"
        ));
    }

    #[test]
    fn test_wildcard_trailing_star() {
        assert!(wildcard_match(
            "https://example.com/*",
            "https://example.com/a/b/c"
        ));
        assert!(wildcard_match("prefix:*", "prefix:anything_at_all"));
    }

    #[test]
    fn test_regex_pattern() {
        let pattern =
            RewritePattern::Regex(r"https://drive\.google\.com/file/d/([^/]+)/view".to_string());
        assert!(pattern.matches("https://drive.google.com/file/d/abc123/view"));
        assert!(!pattern.matches("https://drive.google.com/file/d/abc123/edit"));
    }

    #[test]
    fn test_exact_pattern() {
        let pattern = RewritePattern::Exact("https://example.com/file.zip".to_string());
        assert!(pattern.matches("https://example.com/file.zip"));
        assert!(!pattern.matches("https://example.com/file.zip?token=abc"));
    }

    #[test]
    fn test_prefix_pattern() {
        let pattern = RewritePattern::Prefix("http://".to_string());
        assert!(pattern.matches("http://example.com/file.zip"));
        assert!(!pattern.matches("https://example.com/file.zip"));
    }

    #[test]
    fn test_extract_captures_regex() {
        let pattern =
            RewritePattern::Regex(r"https://drive\.google\.com/file/d/([^/]+)/view".to_string());
        let captures =
            extract_captures(&pattern, "https://drive.google.com/file/d/abc123/view").unwrap();
        assert_eq!(captures.len(), 2); // full match + group 1
        assert_eq!(captures[1], "abc123");
    }

    #[test]
    fn test_extract_captures_wildcard() {
        let pattern = RewritePattern::Wildcard("https://example.com/*/view".to_string());
        let captures = extract_captures(&pattern, "https://example.com/abc123/view").unwrap();
        assert_eq!(captures.len(), 2); // [full_match, group1]
        assert_eq!(captures[0], "https://example.com/abc123/view");
        assert_eq!(captures[1], "abc123");
    }

    #[test]
    fn test_extract_captures_prefix() {
        let pattern = RewritePattern::Prefix("http://".to_string());
        let captures = extract_captures(&pattern, "http://example.com/file.zip").unwrap();
        assert_eq!(captures.len(), 2); // [full_match, group1]
        assert_eq!(captures[0], "http://example.com/file.zip");
        assert_eq!(captures[1], "example.com/file.zip");
    }

    #[test]
    fn test_apply_template() {
        let result = apply_template(
            "https://drive.google.com/uc?export=download&id=$1",
            "https://drive.google.com/file/d/abc123/view",
            &["full_match".to_string(), "abc123".to_string()],
        );
        assert_eq!(
            result,
            "https://drive.google.com/uc?export=download&id=abc123"
        );
    }

    #[test]
    fn test_apply_template_dollar_zero() {
        let result = apply_template("mirror://$0", "https://example.com/file.zip", &[]);
        assert_eq!(result, "mirror://https://example.com/file.zip");
    }

    #[test]
    fn test_url_rewrite_rule_try_rewrite() {
        let rule = UrlRewriteRule {
            id: "gdrive".to_string(),
            name: "Google Drive".to_string(),
            pattern: RewritePattern::Regex(
                r"https://drive\.google\.com/file/d/([^/]+)/view".to_string(),
            ),
            replacement: "https://drive.google.com/uc?export=download&id=$1".to_string(),
            enabled: true,
            priority: 10,
            description: None,
            apply_count: 0,
        };

        let result = rule.try_rewrite("https://drive.google.com/file/d/abc123/view");
        assert_eq!(
            result,
            Some("https://drive.google.com/uc?export=download&id=abc123".to_string())
        );

        let no_match = rule.try_rewrite("https://example.com/file.zip");
        assert_eq!(no_match, None);
    }

    #[test]
    fn test_url_rewrite_rule_disabled() {
        let rule = UrlRewriteRule {
            id: "test".to_string(),
            name: "Test".to_string(),
            pattern: RewritePattern::Wildcard("*".to_string()),
            replacement: "https://rewritten.com".to_string(),
            enabled: false,
            priority: 0,
            description: None,
            apply_count: 0,
        };

        assert_eq!(rule.try_rewrite("https://example.com"), None);
    }

    #[test]
    fn test_url_rewrite_rule_wildcard_rewrite() {
        let rule = UrlRewriteRule {
            id: "http-to-https".to_string(),
            name: "HTTP to HTTPS".to_string(),
            pattern: RewritePattern::Prefix("http://".to_string()),
            replacement: "https://$1".to_string(),
            enabled: true,
            priority: 0,
            description: None,
            apply_count: 0,
        };

        let result = rule.try_rewrite("http://example.com/file.zip");
        assert_eq!(result, Some("https://example.com/file.zip".to_string()));
    }

    #[test]
    fn test_manager_add_and_list() {
        let mut mgr = UrlRewriteManager::new();
        assert!(mgr.rules.is_empty());

        mgr.add_rule(UrlRewriteRule {
            id: "r1".to_string(),
            name: "Rule 1".to_string(),
            pattern: RewritePattern::Wildcard("*".to_string()),
            replacement: "https://test.com".to_string(),
            enabled: true,
            priority: 5,
            description: None,
            apply_count: 0,
        });

        assert_eq!(mgr.list_rules().len(), 1);
    }

    #[test]
    fn test_manager_priority_ordering() {
        let mut mgr = UrlRewriteManager::new();

        mgr.add_rule(UrlRewriteRule {
            id: "low".to_string(),
            name: "Low Priority".to_string(),
            pattern: RewritePattern::Wildcard("*".to_string()),
            replacement: "low".to_string(),
            enabled: true,
            priority: 1,
            description: None,
            apply_count: 0,
        });

        mgr.add_rule(UrlRewriteRule {
            id: "high".to_string(),
            name: "High Priority".to_string(),
            pattern: RewritePattern::Wildcard("*".to_string()),
            replacement: "high".to_string(),
            enabled: true,
            priority: 10,
            description: None,
            apply_count: 0,
        });

        // High priority should be first
        assert_eq!(mgr.rules[0].id, "high");
        assert_eq!(mgr.rules[1].id, "low");
    }

    #[test]
    fn test_manager_rewrite_url_first_match_wins() {
        let mut mgr = UrlRewriteManager::new();

        mgr.add_rule(UrlRewriteRule {
            id: "r1".to_string(),
            name: "First".to_string(),
            pattern: RewritePattern::Prefix("https://example.com/".to_string()),
            replacement: "https://first.com/$1".to_string(),
            enabled: true,
            priority: 10,
            description: None,
            apply_count: 0,
        });

        mgr.add_rule(UrlRewriteRule {
            id: "r2".to_string(),
            name: "Second".to_string(),
            pattern: RewritePattern::Prefix("https://example.com/".to_string()),
            replacement: "https://second.com/$1".to_string(),
            enabled: true,
            priority: 1,
            description: None,
            apply_count: 0,
        });

        let result = mgr.rewrite_url("https://example.com/file.zip");
        assert_eq!(result, "https://first.com/file.zip");
    }

    #[test]
    fn test_manager_rewrite_increments_count() {
        let mut mgr = UrlRewriteManager::new();

        mgr.add_rule(UrlRewriteRule {
            id: "r1".to_string(),
            name: "Test".to_string(),
            pattern: RewritePattern::Wildcard("*".to_string()),
            replacement: "https://rewritten.com".to_string(),
            enabled: true,
            priority: 0,
            description: None,
            apply_count: 0,
        });

        mgr.rewrite_url("https://example.com/a");
        mgr.rewrite_url("https://example.com/b");
        mgr.rewrite_url("https://example.com/c");

        assert_eq!(mgr.rules[0].apply_count, 3);
    }

    #[test]
    fn test_manager_no_match_returns_original() {
        let mut mgr = UrlRewriteManager::new();

        mgr.add_rule(UrlRewriteRule {
            id: "r1".to_string(),
            name: "Specific".to_string(),
            pattern: RewritePattern::Exact("https://specific.com/file".to_string()),
            replacement: "https://other.com".to_string(),
            enabled: true,
            priority: 0,
            description: None,
            apply_count: 0,
        });

        let result = mgr.rewrite_url("https://example.com/file.zip");
        assert_eq!(result, "https://example.com/file.zip");
    }

    #[test]
    fn test_manager_disabled_globally() {
        let mut mgr = UrlRewriteManager::new();
        mgr.enabled = false;

        mgr.add_rule(UrlRewriteRule {
            id: "r1".to_string(),
            name: "Test".to_string(),
            pattern: RewritePattern::Wildcard("*".to_string()),
            replacement: "https://rewritten.com".to_string(),
            enabled: true,
            priority: 0,
            description: None,
            apply_count: 0,
        });

        let result = mgr.rewrite_url("https://example.com");
        assert_eq!(result, "https://example.com");
    }

    #[test]
    fn test_manager_remove_rule() {
        let mut mgr = UrlRewriteManager::new();

        mgr.add_rule(UrlRewriteRule {
            id: "r1".to_string(),
            name: "Test".to_string(),
            pattern: RewritePattern::Wildcard("*".to_string()),
            replacement: "test".to_string(),
            enabled: true,
            priority: 0,
            description: None,
            apply_count: 0,
        });

        assert!(mgr.remove_rule("r1"));
        assert!(!mgr.remove_rule("nonexistent"));
        assert!(mgr.rules.is_empty());
    }

    #[test]
    fn test_manager_get_rule() {
        let mut mgr = UrlRewriteManager::new();

        mgr.add_rule(UrlRewriteRule {
            id: "r1".to_string(),
            name: "Test Rule".to_string(),
            pattern: RewritePattern::Wildcard("*".to_string()),
            replacement: "test".to_string(),
            enabled: true,
            priority: 0,
            description: None,
            apply_count: 0,
        });

        let rule = mgr.get_rule("r1").unwrap();
        assert_eq!(rule.name, "Test Rule");
        assert!(mgr.get_rule("nonexistent").is_none());
    }

    #[test]
    fn test_preview_rewrite() {
        let mut mgr = UrlRewriteManager::new();

        mgr.add_rule(UrlRewriteRule {
            id: "r1".to_string(),
            name: "Test Rule".to_string(),
            pattern: RewritePattern::Wildcard("*".to_string()),
            replacement: "https://rewritten.com".to_string(),
            enabled: true,
            priority: 0,
            description: None,
            apply_count: 0,
        });

        let (url, rule_name) = mgr.preview_rewrite("https://example.com").unwrap();
        assert_eq!(url, "https://rewritten.com");
        assert_eq!(rule_name, "Test Rule");

        // Preview should NOT increment apply count
        assert_eq!(mgr.rules[0].apply_count, 0);
    }

    #[test]
    fn test_preview_rewrite_no_match() {
        let mgr = UrlRewriteManager::new();
        assert!(mgr.preview_rewrite("https://example.com").is_none());
    }

    #[test]
    fn test_summary() {
        let mut mgr = UrlRewriteManager::new();

        mgr.add_rule(UrlRewriteRule {
            id: "r1".to_string(),
            name: "Rule 1".to_string(),
            pattern: RewritePattern::Wildcard("*".to_string()),
            replacement: "test".to_string(),
            enabled: true,
            priority: 0,
            description: None,
            apply_count: 5,
        });

        mgr.add_rule(UrlRewriteRule {
            id: "r2".to_string(),
            name: "Rule 2".to_string(),
            pattern: RewritePattern::Exact("test".to_string()),
            replacement: "test2".to_string(),
            enabled: false,
            priority: 0,
            description: None,
            apply_count: 3,
        });

        let summary = mgr.summary();
        assert!(summary.global_enabled);
        assert_eq!(summary.total_rules, 2);
        assert_eq!(summary.enabled_rules, 1);
        assert_eq!(summary.total_applies, 8);
        assert_eq!(summary.rules.len(), 2);
    }

    #[test]
    fn test_format_report() {
        let mut mgr = UrlRewriteManager::new();
        mgr.add_rule(UrlRewriteRule {
            id: "r1".to_string(),
            name: "Google Drive".to_string(),
            pattern: RewritePattern::Regex(
                r"https://drive\.google\.com/file/d/([^/]+)/view".to_string(),
            ),
            replacement: "https://drive.google.com/uc?export=download&id=$1".to_string(),
            enabled: true,
            priority: 10,
            description: None,
            apply_count: 42,
        });

        let report = mgr.summary().format_report();
        assert!(report.contains("URL Rewrite Rules"));
        assert!(report.contains("Google Drive"));
        assert!(report.contains("42"));
    }

    #[test]
    fn test_format_report_empty() {
        let mgr = UrlRewriteManager::new();
        let report = mgr.summary().format_report();
        assert!(report.contains("no rules configured"));
    }

    #[test]
    fn test_parse_pattern_wildcard() {
        let p = parse_pattern("https://example.com/*");
        assert!(matches!(p, RewritePattern::Wildcard(_)));
        assert!(p.matches("https://example.com/anything"));
    }

    #[test]
    fn test_parse_pattern_regex() {
        let p = parse_pattern("regex:https://example\\.com/.*");
        assert!(matches!(p, RewritePattern::Regex(_)));
        assert!(p.matches("https://example.com/anything"));
    }

    #[test]
    fn test_parse_pattern_exact() {
        let p = parse_pattern("exact:https://example.com/file.zip");
        assert!(matches!(p, RewritePattern::Exact(_)));
        assert!(p.matches("https://example.com/file.zip"));
        assert!(!p.matches("https://example.com/other.zip"));
    }

    #[test]
    fn test_parse_pattern_prefix() {
        let p = parse_pattern("prefix:http://");
        assert!(matches!(p, RewritePattern::Prefix(_)));
        assert!(p.matches("http://example.com"));
        assert!(!p.matches("https://example.com"));
    }

    #[test]
    fn test_save_load_url_rewrite_manager() {
        let temp_dir = std::env::temp_dir().join("test_url_rewrite_save_load");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let mut mgr = UrlRewriteManager::new();
        mgr.add_rule(UrlRewriteRule {
            id: "r1".to_string(),
            name: "Test".to_string(),
            pattern: RewritePattern::Wildcard("*".to_string()),
            replacement: "https://test.com".to_string(),
            enabled: true,
            priority: 5,
            description: Some("A test rule".to_string()),
            apply_count: 10,
        });

        save_url_rewrite_manager(&mgr, &temp_dir).unwrap();
        let loaded = load_url_rewrite_manager(&temp_dir).unwrap();

        assert_eq!(loaded.rules.len(), 1);
        assert_eq!(loaded.rules[0].id, "r1");
        assert_eq!(loaded.rules[0].name, "Test");
        assert_eq!(loaded.rules[0].apply_count, 10);
        assert!(loaded.enabled);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_load_missing_file() {
        let temp_dir = std::env::temp_dir().join("test_url_rewrite_missing");
        let _ = std::fs::remove_dir_all(&temp_dir);
        assert!(load_url_rewrite_manager(&temp_dir).is_none());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut mgr = UrlRewriteManager::new();
        mgr.add_rule(UrlRewriteRule {
            id: "r1".to_string(),
            name: "GDrive".to_string(),
            pattern: RewritePattern::Regex(
                r"https://drive\.google\.com/file/d/([^/]+)/view".to_string(),
            ),
            replacement: "https://drive.google.com/uc?export=download&id=$1".to_string(),
            enabled: true,
            priority: 10,
            description: None,
            apply_count: 0,
        });

        let json = serde_json::to_string(&mgr).unwrap();
        let deserialized: UrlRewriteManager = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.rules.len(), 1);
        assert_eq!(deserialized.rules[0].id, "r1");
        assert!(deserialized.enabled);
    }

    #[test]
    fn test_google_drive_rewrite() {
        let mut mgr = UrlRewriteManager::new();
        mgr.add_rule(UrlRewriteRule {
            id: "gdrive".to_string(),
            name: "Google Drive Direct Link".to_string(),
            pattern: RewritePattern::Regex(
                r"https://drive\.google\.com/file/d/([^/]+)/view".to_string(),
            ),
            replacement: "https://drive.google.com/uc?export=download&id=$1".to_string(),
            enabled: true,
            priority: 10,
            description: Some("Convert GDrive view links to direct download".to_string()),
            apply_count: 0,
        });

        let result = mgr.rewrite_url("https://drive.google.com/file/d/1ABCxyz123/view");
        assert_eq!(
            result,
            "https://drive.google.com/uc?export=download&id=1ABCxyz123"
        );
    }

    #[test]
    fn test_domain_rewrite() {
        let mut mgr = UrlRewriteManager::new();
        mgr.add_rule(UrlRewriteRule {
            id: "domain-swap".to_string(),
            name: "Old to New Domain".to_string(),
            pattern: RewritePattern::Wildcard("https://old-site.com/*".to_string()),
            replacement: "https://new-site.com/$1".to_string(),
            enabled: true,
            priority: 0,
            description: None,
            apply_count: 0,
        });

        let result = mgr.rewrite_url("https://old-site.com/files/archive.zip");
        assert_eq!(result, "https://new-site.com/files/archive.zip");
    }

    #[test]
    fn test_wildcard_no_capture_groups() {
        // Wildcard with no * should still work
        let rule = UrlRewriteRule {
            id: "exact-ish".to_string(),
            name: "No Stars".to_string(),
            pattern: RewritePattern::Wildcard("https://example.com/file.zip".to_string()),
            replacement: "https://mirror.com/file.zip".to_string(),
            enabled: true,
            priority: 0,
            description: None,
            apply_count: 0,
        };

        let result = rule.try_rewrite("https://example.com/file.zip");
        assert_eq!(result, Some("https://mirror.com/file.zip".to_string()));
    }

    #[test]
    fn test_manager_get_rule_mut() {
        let mut mgr = UrlRewriteManager::new();
        mgr.add_rule(UrlRewriteRule {
            id: "r1".to_string(),
            name: "Test".to_string(),
            pattern: RewritePattern::Wildcard("*".to_string()),
            replacement: "test".to_string(),
            enabled: true,
            priority: 0,
            description: None,
            apply_count: 0,
        });

        let rule = mgr.get_rule_mut("r1").unwrap();
        rule.enabled = false;

        assert!(!mgr.rules[0].enabled);
    }
}
