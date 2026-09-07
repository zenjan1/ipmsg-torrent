//! URL Pattern Batch Download
//!
//! Expand URL patterns with numeric ranges into individual URLs.
//! Supports patterns like:
//! - `http://example.com/file{01-99}.zip` → 99 URLs
//! - `http://example.com/img_{001-100}.png` → 100 URLs with zero-padding
//! - `http://example.com/{a-d}.txt` → 4 URLs (a, b, c, d)
//! - Multiple ranges: `http://example.com/{1-3}_{a-b}.txt` → 6 URLs
//!
//! Features:
//! - Numeric ranges with optional zero-padding
//! - Alphabetic ranges (a-z, A-Z)
//! - Multiple patterns in single URL
//! - Custom step size
//! - Validation and error handling

use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::Path;

/// Error type for URL pattern expansion
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatternError {
    /// Pattern syntax error (unclosed brace, invalid range, etc.)
    SyntaxError(String),
    /// Range is invalid (start > end, empty range, etc.)
    InvalidRange(String),
    /// Too many URLs would be generated (safety limit)
    TooManyUrls { generated: usize, limit: usize },
}

impl fmt::Display for PatternError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatternError::SyntaxError(msg) => write!(f, "Pattern syntax error: {}", msg),
            PatternError::InvalidRange(msg) => write!(f, "Invalid range: {}", msg),
            PatternError::TooManyUrls { generated, limit } => {
                write!(
                    f,
                    "Too many URLs: {} would be generated (limit: {})",
                    generated, limit
                )
            }
        }
    }
}

impl std::error::Error for PatternError {}

/// A single pattern segment within a URL
#[derive(Debug, Clone, PartialEq, Eq)]
enum PatternSegment {
    /// Literal text (no pattern)
    Literal(String),
    /// Numeric range: {start-end} or {start-end:step}
    NumericRange {
        start: u64,
        end: u64,
        step: u64,
        pad_width: Option<usize>,
    },
    /// Alphabetic range: {a-z} or {A-Z}
    AlphaRange { start: char, end: char, step: u8 },
}

/// Configuration for URL pattern expansion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternConfig {
    /// Maximum number of URLs to generate (safety limit)
    pub max_urls: usize,
    /// Default step size for ranges (if not specified in pattern)
    pub default_step: u64,
}

impl Default for PatternConfig {
    fn default() -> Self {
        Self {
            max_urls: 1000,
            default_step: 1,
        }
    }
}

/// Result of expanding a URL pattern
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternExpansionResult {
    /// Expanded URLs
    pub urls: Vec<String>,
    /// Original pattern
    pub pattern: String,
    /// Number of URLs generated
    pub count: usize,
    /// Whether the pattern was truncated due to limits
    pub truncated: bool,
}

/// Parse a pattern string into segments
fn parse_pattern(pattern: &str) -> Result<Vec<PatternSegment>, PatternError> {
    let mut segments = Vec::new();
    let mut chars = pattern.chars().peekable();
    let mut literal = String::new();

    while let Some(ch) = chars.next() {
        if ch == '{' {
            // Save any accumulated literal
            if !literal.is_empty() {
                segments.push(PatternSegment::Literal(literal.clone()));
                literal.clear();
            }

            // Parse pattern content until '}'
            let mut pattern_content = String::new();
            let mut found_close = false;
            for inner_ch in chars.by_ref() {
                if inner_ch == '}' {
                    found_close = true;
                    break;
                }
                pattern_content.push(inner_ch);
            }

            if !found_close {
                return Err(PatternError::SyntaxError(
                    "Unclosed brace in pattern".to_string(),
                ));
            }

            // Parse the pattern content
            let segment = parse_pattern_content(&pattern_content)?;
            segments.push(segment);
        } else {
            literal.push(ch);
        }
    }

    // Save any remaining literal
    if !literal.is_empty() {
        segments.push(PatternSegment::Literal(literal));
    }

    Ok(segments)
}

/// Parse the content inside braces
fn parse_pattern_content(content: &str) -> Result<PatternSegment, PatternError> {
    if content.is_empty() {
        return Err(PatternError::SyntaxError("Empty pattern".to_string()));
    }

    // Check for step specification: {start-end:step}
    let (range_part, step_override) = if let Some(colon_pos) = content.rfind(':') {
        let step_str = &content[colon_pos + 1..];
        let step = step_str
            .parse::<u64>()
            .map_err(|_| PatternError::SyntaxError(format!("Invalid step: {}", step_str)))?;
        if step == 0 {
            return Err(PatternError::InvalidRange(
                "Step cannot be zero".to_string(),
            ));
        }
        (&content[..colon_pos], Some(step))
    } else {
        (content, None)
    };

    // Try to parse as numeric range first
    if let Some(dash_pos) = range_part.find('-') {
        // Check if it's a negative number (unlikely in URLs but handle it)
        if dash_pos == 0 {
            return Err(PatternError::SyntaxError(
                "Invalid range format".to_string(),
            ));
        }

        let start_str = &range_part[..dash_pos];
        let end_str = &range_part[dash_pos + 1..];

        // Try numeric parsing
        if let (Ok(start), Ok(end)) = (start_str.parse::<u64>(), end_str.parse::<u64>()) {
            // Determine padding width from the start number
            let pad_width = if start_str.starts_with('0') && start_str.len() > 1 {
                Some(start_str.len())
            } else if end_str.starts_with('0') && end_str.len() > 1 {
                Some(end_str.len())
            } else {
                None
            };

            if start > end {
                return Err(PatternError::InvalidRange(format!(
                    "Start ({}) > end ({})",
                    start, end
                )));
            }

            let step = step_override.unwrap_or(1);
            return Ok(PatternSegment::NumericRange {
                start,
                end,
                step,
                pad_width,
            });
        }

        // Try alphabetic range (single characters)
        if start_str.len() == 1 && end_str.len() == 1 {
            let start_char = start_str.chars().next().unwrap();
            let end_char = end_str.chars().next().unwrap();

            if start_char.is_ascii_alphabetic() && end_char.is_ascii_alphabetic() {
                // Check case consistency
                if start_char.is_lowercase() != end_char.is_lowercase() {
                    return Err(PatternError::InvalidRange(
                        "Mixed case in alphabetic range".to_string(),
                    ));
                }

                if start_char > end_char {
                    return Err(PatternError::InvalidRange(format!(
                        "Start ('{}') > end ('{}')",
                        start_char, end_char
                    )));
                }

                let step = step_override.unwrap_or(1) as u8;
                return Ok(PatternSegment::AlphaRange {
                    start: start_char,
                    end: end_char,
                    step,
                });
            }
        }
    }

    Err(PatternError::SyntaxError(format!(
        "Invalid pattern: {}",
        content
    )))
}

/// Expand a URL pattern into individual URLs
pub fn expand_pattern(pattern: &str) -> Result<Vec<String>, PatternError> {
    expand_pattern_with_config(pattern, &PatternConfig::default())
}

/// Expand a URL pattern with custom configuration
pub fn expand_pattern_with_config(
    pattern: &str,
    config: &PatternConfig,
) -> Result<Vec<String>, PatternError> {
    let segments = parse_pattern(pattern)?;

    // Calculate total combinations first
    let mut total: usize = 1;
    for segment in &segments {
        let count = match segment {
            PatternSegment::Literal(_) => 1,
            PatternSegment::NumericRange {
                start, end, step, ..
            } => ((end - start) / step + 1) as usize,
            PatternSegment::AlphaRange { start, end, step } => {
                ((*end as u8 - *start as u8) / step + 1) as usize
            }
        };
        total = total.saturating_mul(count);
    }

    // Check safety limit
    if total > config.max_urls {
        return Err(PatternError::TooManyUrls {
            generated: total,
            limit: config.max_urls,
        });
    }

    // Generate all combinations
    let mut urls = Vec::with_capacity(total);
    generate_combinations(&segments, 0, String::new(), &mut urls);

    Ok(urls)
}

/// Recursively generate all combinations of pattern segments
fn generate_combinations(
    segments: &[PatternSegment],
    index: usize,
    prefix: String,
    results: &mut Vec<String>,
) {
    if index >= segments.len() {
        results.push(prefix);
        return;
    }

    match &segments[index] {
        PatternSegment::Literal(text) => {
            let new_prefix = format!("{}{}", prefix, text);
            generate_combinations(segments, index + 1, new_prefix, results);
        }
        PatternSegment::NumericRange {
            start,
            end,
            step,
            pad_width,
        } => {
            let mut current = *start;
            while current <= *end {
                let formatted = if let Some(width) = pad_width {
                    format!("{:0>width$}", current, width = width)
                } else {
                    current.to_string()
                };
                let new_prefix = format!("{}{}", prefix, formatted);
                generate_combinations(segments, index + 1, new_prefix, results);
                current += step;
            }
        }
        PatternSegment::AlphaRange { start, end, step } => {
            let mut current = *start as u8;
            while current <= *end as u8 {
                let ch = current as char;
                let new_prefix = format!("{}{}", prefix, ch);
                generate_combinations(segments, index + 1, new_prefix, results);
                current += step;
            }
        }
    }
}

/// Check if a string contains URL patterns
pub fn contains_pattern(s: &str) -> bool {
    s.contains('{') && s.contains('}')
}

/// Save pattern configuration to disk (atomic write)
pub fn save_pattern_config(config: &PatternConfig, data_dir: &Path) -> Result<(), String> {
    let path = data_dir.join("url_pattern_config.json");
    let json = serde_json::to_string_pretty(config).map_err(|e| e.to_string())?;
    std::fs::write(&path, json).map_err(|e| e.to_string())
}

/// Load pattern configuration from disk
pub fn load_pattern_config(data_dir: &Path) -> PatternConfig {
    let path = data_dir.join("url_pattern_config.json");
    match std::fs::read_to_string(&path) {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => PatternConfig::default(),
    }
}

/// Validate a URL pattern without expanding it
pub fn validate_pattern(pattern: &str) -> Result<(), PatternError> {
    parse_pattern(pattern)?;
    Ok(())
}

/// Estimate the number of URLs a pattern would generate
pub fn estimate_count(pattern: &str) -> Result<usize, PatternError> {
    let segments = parse_pattern(pattern)?;
    let mut total: usize = 1;
    for segment in &segments {
        let count = match segment {
            PatternSegment::Literal(_) => 1,
            PatternSegment::NumericRange {
                start, end, step, ..
            } => ((end - start) / step + 1) as usize,
            PatternSegment::AlphaRange { start, end, step } => {
                ((*end as u8 - *start as u8) / step + 1) as usize
            }
        };
        total = total.saturating_mul(count);
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_only() {
        let urls = expand_pattern("http://example.com/file.txt").unwrap();
        assert_eq!(urls, vec!["http://example.com/file.txt"]);
    }

    #[test]
    fn test_numeric_range_basic() {
        let urls = expand_pattern("http://example.com/file{1-3}.txt").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/file1.txt",
                "http://example.com/file2.txt",
                "http://example.com/file3.txt",
            ]
        );
    }

    #[test]
    fn test_numeric_range_zero_padded() {
        let urls = expand_pattern("http://example.com/file{01-03}.txt").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/file01.txt",
                "http://example.com/file02.txt",
                "http://example.com/file03.txt",
            ]
        );
    }

    #[test]
    fn test_numeric_range_with_step() {
        let urls = expand_pattern("http://example.com/file{1-10:3}.txt").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/file1.txt",
                "http://example.com/file4.txt",
                "http://example.com/file7.txt",
                "http://example.com/file10.txt",
            ]
        );
    }

    #[test]
    fn test_numeric_range_large() {
        let urls = expand_pattern("http://example.com/file{001-100}.zip").unwrap();
        assert_eq!(urls.len(), 100);
        assert_eq!(urls[0], "http://example.com/file001.zip");
        assert_eq!(urls[99], "http://example.com/file100.zip");
    }

    #[test]
    fn test_alpha_range_lowercase() {
        let urls = expand_pattern("http://example.com/{a-c}.txt").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/a.txt",
                "http://example.com/b.txt",
                "http://example.com/c.txt",
            ]
        );
    }

    #[test]
    fn test_alpha_range_uppercase() {
        let urls = expand_pattern("http://example.com/{X-Z}.txt").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/X.txt",
                "http://example.com/Y.txt",
                "http://example.com/Z.txt",
            ]
        );
    }

    #[test]
    fn test_alpha_range_with_step() {
        let urls = expand_pattern("http://example.com/{a-f:2}.txt").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/a.txt",
                "http://example.com/c.txt",
                "http://example.com/e.txt",
            ]
        );
    }

    #[test]
    fn test_multiple_patterns() {
        let urls = expand_pattern("http://example.com/{1-2}_{a-b}.txt").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/1_a.txt",
                "http://example.com/1_b.txt",
                "http://example.com/2_a.txt",
                "http://example.com/2_b.txt",
            ]
        );
    }

    #[test]
    fn test_mixed_patterns() {
        let urls = expand_pattern("http://example.com/{a-b}{1-2}.txt").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/a1.txt",
                "http://example.com/a2.txt",
                "http://example.com/b1.txt",
                "http://example.com/b2.txt",
            ]
        );
    }

    #[test]
    fn test_error_unclosed_brace() {
        let result = expand_pattern("http://example.com/file{1-3.txt");
        assert!(matches!(result, Err(PatternError::SyntaxError(_))));
    }

    #[test]
    fn test_error_empty_pattern() {
        let result = expand_pattern("http://example.com/file{}.txt");
        assert!(matches!(result, Err(PatternError::SyntaxError(_))));
    }

    #[test]
    fn test_error_invalid_range_reversed() {
        let result = expand_pattern("http://example.com/file{5-1}.txt");
        assert!(matches!(result, Err(PatternError::InvalidRange(_))));
    }

    #[test]
    fn test_error_mixed_case_alpha() {
        let result = expand_pattern("http://example.com/{a-B}.txt");
        assert!(matches!(result, Err(PatternError::InvalidRange(_))));
    }

    #[test]
    fn test_error_zero_step() {
        let result = expand_pattern("http://example.com/file{1-10:0}.txt");
        assert!(matches!(result, Err(PatternError::InvalidRange(_))));
    }

    #[test]
    fn test_error_too_many_urls() {
        let config = PatternConfig {
            max_urls: 10,
            default_step: 1,
        };
        let result = expand_pattern_with_config("http://example.com/file{1-100}.txt", &config);
        assert!(matches!(
            result,
            Err(PatternError::TooManyUrls {
                generated: 100,
                limit: 10
            })
        ));
    }

    #[test]
    fn test_contains_pattern() {
        assert!(contains_pattern("http://example.com/file{1-3}.txt"));
        assert!(!contains_pattern("http://example.com/file.txt"));
        assert!(contains_pattern("{a-z}"));
        assert!(!contains_pattern("no braces here"));
    }

    #[test]
    fn test_validate_pattern() {
        assert!(validate_pattern("http://example.com/file{1-3}.txt").is_ok());
        assert!(validate_pattern("http://example.com/file.txt").is_ok());
        assert!(validate_pattern("http://example.com/file{1-3.txt").is_err());
    }

    #[test]
    fn test_estimate_count() {
        assert_eq!(
            estimate_count("http://example.com/file{1-10}.txt").unwrap(),
            10
        );
        assert_eq!(
            estimate_count("http://example.com/file{01-100}.txt").unwrap(),
            100
        );
        assert_eq!(estimate_count("http://example.com/{a-z}.txt").unwrap(), 26);
        assert_eq!(
            estimate_count("http://example.com/{1-5}_{a-e}.txt").unwrap(),
            25
        );
    }

    #[test]
    fn test_single_value_range() {
        let urls = expand_pattern("http://example.com/file{5-5}.txt").unwrap();
        assert_eq!(urls, vec!["http://example.com/file5.txt"]);
    }

    #[test]
    fn test_padding_width_from_end() {
        // If end has padding but start doesn't, use end's width
        let urls = expand_pattern("http://example.com/file{1-003}.txt").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/file001.txt",
                "http://example.com/file002.txt",
                "http://example.com/file003.txt",
            ]
        );
    }

    #[test]
    fn test_real_world_example() {
        // Real-world example: downloading manga chapters
        let urls =
            expand_pattern("http://manga.example.com/chapter_{001-003}/page_{01-05}.jpg").unwrap();
        assert_eq!(urls.len(), 15); // 3 chapters × 5 pages
        assert_eq!(urls[0], "http://manga.example.com/chapter_001/page_01.jpg");
        assert_eq!(urls[14], "http://manga.example.com/chapter_003/page_05.jpg");
    }

    // ── Phase 252: Comprehensive test coverage ──

    // PatternError Display
    #[test]
    fn test_pattern_error_display_syntax() {
        let err = PatternError::SyntaxError("unclosed brace".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Pattern syntax error"));
        assert!(msg.contains("unclosed brace"));
    }

    #[test]
    fn test_pattern_error_display_invalid_range() {
        let err = PatternError::InvalidRange("start > end".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Invalid range"));
        assert!(msg.contains("start > end"));
    }

    #[test]
    fn test_pattern_error_display_too_many_urls() {
        let err = PatternError::TooManyUrls {
            generated: 5000,
            limit: 1000,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("Too many URLs"));
        assert!(msg.contains("5000"));
        assert!(msg.contains("1000"));
    }

    // PatternError traits
    #[test]
    fn test_pattern_error_debug() {
        let err = PatternError::SyntaxError("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("SyntaxError"));
    }

    #[test]
    fn test_pattern_error_clone_eq() {
        let err1 = PatternError::SyntaxError("abc".to_string());
        let err2 = err1.clone();
        assert_eq!(err1, err2);
    }

    #[test]
    fn test_pattern_error_eq_variants() {
        let a = PatternError::SyntaxError("x".to_string());
        let b = PatternError::SyntaxError("x".to_string());
        let c = PatternError::SyntaxError("y".to_string());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_pattern_error_is_std_error() {
        let err: Box<dyn std::error::Error> =
            Box::new(PatternError::SyntaxError("test".to_string()));
        assert!(err.to_string().contains("Pattern syntax error"));
    }

    #[test]
    fn test_pattern_error_unicode_message() {
        let err = PatternError::SyntaxError("中文错误消息".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("中文错误消息"));
    }

    // PatternConfig
    #[test]
    fn test_pattern_config_default() {
        let config = PatternConfig::default();
        assert_eq!(config.max_urls, 1000);
        assert_eq!(config.default_step, 1);
    }

    #[test]
    fn test_pattern_config_serde_roundtrip() {
        let config = PatternConfig {
            max_urls: 500,
            default_step: 2,
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: PatternConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_urls, 500);
        assert_eq!(deserialized.default_step, 2);
    }

    #[test]
    fn test_pattern_config_serde_extra_fields_ignored() {
        let json = r#"{"max_urls":100,"default_step":3,"extra_field":"ignored"}"#;
        let config: PatternConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_urls, 100);
        assert_eq!(config.default_step, 3);
    }

    #[test]
    fn test_pattern_config_clone_debug() {
        let config = PatternConfig {
            max_urls: 200,
            default_step: 5,
        };
        let cloned = config.clone();
        assert_eq!(cloned.max_urls, 200);
        let debug = format!("{:?}", config);
        assert!(debug.contains("max_urls"));
    }

    // PatternExpansionResult
    #[test]
    fn test_pattern_expansion_result_serde_roundtrip() {
        let result = PatternExpansionResult {
            urls: vec!["http://a.com/1.txt".to_string(), "http://a.com/2.txt".to_string()],
            pattern: "http://a.com/{1-2}.txt".to_string(),
            count: 2,
            truncated: false,
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: PatternExpansionResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.urls.len(), 2);
        assert_eq!(deserialized.count, 2);
        assert!(!deserialized.truncated);
    }

    #[test]
    fn test_pattern_expansion_result_clone_debug() {
        let result = PatternExpansionResult {
            urls: vec!["url1".to_string()],
            pattern: "pat".to_string(),
            count: 1,
            truncated: false,
        };
        let cloned = result.clone();
        assert_eq!(cloned.pattern, "pat");
        let debug = format!("{:?}", result);
        assert!(debug.contains("pattern"));
    }

    // expand_pattern boundary tests
    #[test]
    fn test_expand_numeric_starting_from_zero() {
        let urls = expand_pattern("http://example.com/file{0-3}.txt").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/file0.txt",
                "http://example.com/file1.txt",
                "http://example.com/file2.txt",
                "http://example.com/file3.txt",
            ]
        );
    }

    #[test]
    fn test_expand_alpha_single_char() {
        let urls = expand_pattern("http://example.com/{a-a}.txt").unwrap();
        assert_eq!(urls, vec!["http://example.com/a.txt"]);
    }

    #[test]
    fn test_expand_alpha_full_lowercase() {
        let urls = expand_pattern("http://example.com/{a-z}.txt").unwrap();
        assert_eq!(urls.len(), 26);
        assert_eq!(urls[0], "http://example.com/a.txt");
        assert_eq!(urls[25], "http://example.com/z.txt");
    }

    #[test]
    fn test_expand_alpha_full_uppercase() {
        let urls = expand_pattern("http://example.com/{A-Z}.txt").unwrap();
        assert_eq!(urls.len(), 26);
        assert_eq!(urls[0], "http://example.com/A.txt");
        assert_eq!(urls[25], "http://example.com/Z.txt");
    }

    #[test]
    fn test_expand_large_numeric_no_padding() {
        let urls = expand_pattern("http://example.com/{999-1002}.dat").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/999.dat",
                "http://example.com/1000.dat",
                "http://example.com/1001.dat",
                "http://example.com/1002.dat",
            ]
        );
    }

    #[test]
    fn test_expand_numeric_step_larger_than_range() {
        // step=100 but range is 1-10 → only 1 value
        let urls = expand_pattern("http://example.com/file{1-10:100}.txt").unwrap();
        assert_eq!(urls, vec!["http://example.com/file1.txt"]);
    }

    #[test]
    fn test_expand_alpha_step_3() {
        let urls = expand_pattern("http://example.com/{a-z:3}.txt").unwrap();
        // a, d, g, j, m, p, s, v, y
        assert_eq!(urls.len(), 9);
        assert_eq!(urls[0], "http://example.com/a.txt");
        assert_eq!(urls[1], "http://example.com/d.txt");
        assert_eq!(urls[8], "http://example.com/y.txt");
    }

    #[test]
    fn test_expand_empty_pattern_string() {
        let urls = expand_pattern("").unwrap();
        assert_eq!(urls, vec![""]);
    }

    #[test]
    fn test_expand_pattern_no_braces() {
        let urls = expand_pattern("http://example.com/plain.txt").unwrap();
        assert_eq!(urls, vec!["http://example.com/plain.txt"]);
    }

    #[test]
    fn test_expand_pattern_with_query_params() {
        let urls = expand_pattern("http://example.com/file{1-3}.txt?v=1").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/file1.txt?v=1",
                "http://example.com/file2.txt?v=1",
                "http://example.com/file3.txt?v=1",
            ]
        );
    }

    #[test]
    fn test_expand_pattern_with_fragment() {
        let urls = expand_pattern("http://example.com/page{1-2}.html#section").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/page1.html#section",
                "http://example.com/page2.html#section",
            ]
        );
    }

    #[test]
    fn test_expand_three_patterns_in_one_url() {
        let urls = expand_pattern("http://example.com/{a-b}/{1-2}/{x-y}.txt").unwrap();
        // 2 × 2 × 2 = 8 combinations
        assert_eq!(urls.len(), 8);
        assert_eq!(urls[0], "http://example.com/a/1/x.txt");
        assert_eq!(urls[7], "http://example.com/b/2/y.txt");
    }

    #[test]
    fn test_expand_unicode_in_literal_parts() {
        let urls = expand_pattern("http://example.com/中文/file{1-2}.txt").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/中文/file1.txt",
                "http://example.com/中文/file2.txt",
            ]
        );
    }

    #[test]
    fn test_expand_special_chars_in_url() {
        let urls = expand_pattern("http://example.com/file{1-2}.tar.gz").unwrap();
        assert_eq!(
            urls,
            vec![
                "http://example.com/file1.tar.gz",
                "http://example.com/file2.tar.gz",
            ]
        );
    }

    // parse_pattern edge cases
    #[test]
    fn test_parse_pattern_negative_start() {
        // dash at position 0 is a syntax error
        let result = expand_pattern("http://example.com/file{-5}.txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_pattern_only_literal_text() {
        let urls = expand_pattern("just plain text").unwrap();
        assert_eq!(urls, vec!["just plain text"]);
    }

    #[test]
    fn test_parse_pattern_adjacent_braces() {
        let urls = expand_pattern("{1-2}{a-b}").unwrap();
        assert_eq!(urls, vec!["1a", "1b", "2a", "2b"]);
    }

    // contains_pattern edge cases
    #[test]
    fn test_contains_pattern_empty_string() {
        assert!(!contains_pattern(""));
    }

    #[test]
    fn test_contains_pattern_only_open_brace() {
        assert!(!contains_pattern("hello{"));
    }

    #[test]
    fn test_contains_pattern_only_close_brace() {
        assert!(!contains_pattern("hello}"));
    }

    #[test]
    fn test_contains_pattern_nested_braces() {
        assert!(contains_pattern("{{nested}}"));
    }

    // validate_pattern
    #[test]
    fn test_validate_pattern_empty_string() {
        assert!(validate_pattern("").is_ok());
    }

    #[test]
    fn test_validate_pattern_valid_alpha() {
        assert!(validate_pattern("{a-z}").is_ok());
    }

    #[test]
    fn test_validate_pattern_invalid_step_zero() {
        assert!(validate_pattern("{1-10:0}").is_err());
    }

    // estimate_count
    #[test]
    fn test_estimate_count_literal_only() {
        assert_eq!(estimate_count("http://example.com/file.txt").unwrap(), 1);
    }

    #[test]
    fn test_estimate_count_single_range() {
        assert_eq!(estimate_count("{1-100}").unwrap(), 100);
    }

    #[test]
    fn test_estimate_count_with_step() {
        // {1-10:2} → 1,3,5,7,9 = 5 values
        assert_eq!(estimate_count("{1-10:2}").unwrap(), 5);
    }

    #[test]
    fn test_estimate_count_alpha_full() {
        assert_eq!(estimate_count("{a-z}").unwrap(), 26);
    }

    #[test]
    fn test_estimate_count_multiple_patterns() {
        // {1-10} × {a-e} = 10 × 5 = 50
        assert_eq!(estimate_count("{1-10}{a-e}").unwrap(), 50);
    }

    #[test]
    fn test_estimate_count_empty_string() {
        assert_eq!(estimate_count("").unwrap(), 1);
    }

    // Persistence
    #[test]
    fn test_save_pattern_config_creates_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = PatternConfig {
            max_urls: 500,
            default_step: 3,
        };
        save_pattern_config(&config, temp_dir.path()).unwrap();
        let path = temp_dir.path().join("url_pattern_config.json");
        assert!(path.exists());
    }

    #[test]
    fn test_save_and_load_pattern_config_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = PatternConfig {
            max_urls: 777,
            default_step: 7,
        };
        save_pattern_config(&config, temp_dir.path()).unwrap();
        let loaded = load_pattern_config(temp_dir.path());
        assert_eq!(loaded.max_urls, 777);
        assert_eq!(loaded.default_step, 7);
    }

    #[test]
    fn test_load_pattern_config_missing_file_returns_default() {
        let temp_dir = tempfile::tempdir().unwrap();
        let loaded = load_pattern_config(temp_dir.path());
        assert_eq!(loaded.max_urls, 1000);
        assert_eq!(loaded.default_step, 1);
    }

    #[test]
    fn test_load_pattern_config_corrupted_json_returns_default() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("url_pattern_config.json");
        std::fs::write(&path, "not valid json{{{").unwrap();
        let loaded = load_pattern_config(temp_dir.path());
        assert_eq!(loaded.max_urls, 1000);
        assert_eq!(loaded.default_step, 1);
    }

    #[test]
    fn test_save_pattern_config_overwrites_existing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config1 = PatternConfig {
            max_urls: 100,
            default_step: 1,
        };
        save_pattern_config(&config1, temp_dir.path()).unwrap();

        let config2 = PatternConfig {
            max_urls: 999,
            default_step: 5,
        };
        save_pattern_config(&config2, temp_dir.path()).unwrap();

        let loaded = load_pattern_config(temp_dir.path());
        assert_eq!(loaded.max_urls, 999);
        assert_eq!(loaded.default_step, 5);
    }

    // expand_pattern_with_config
    #[test]
    fn test_expand_with_custom_max_urls_exact_boundary() {
        let config = PatternConfig {
            max_urls: 5,
            default_step: 1,
        };
        // Exactly 5 URLs should succeed
        let result = expand_pattern_with_config("{1-5}", &config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 5);
    }

    #[test]
    fn test_expand_with_custom_max_urls_just_over() {
        let config = PatternConfig {
            max_urls: 5,
            default_step: 1,
        };
        // 6 URLs should fail
        let result = expand_pattern_with_config("{1-6}", &config);
        assert!(matches!(
            result,
            Err(PatternError::TooManyUrls { .. })
        ));
    }

    // Error messages
    #[test]
    fn test_error_message_start_gt_end() {
        let result = expand_pattern("{10-5}");
        match result {
            Err(PatternError::InvalidRange(msg)) => {
                assert!(msg.contains("10"));
                assert!(msg.contains("5"));
            }
            _ => panic!("expected InvalidRange"),
        }
    }

    #[test]
    fn test_error_message_mixed_case() {
        let result = expand_pattern("{a-B}");
        match result {
            Err(PatternError::InvalidRange(msg)) => {
                assert!(msg.contains("Mixed case"));
            }
            _ => panic!("expected InvalidRange, got {:?}", result),
        }
    }

    #[test]
    fn test_error_message_unclosed_brace() {
        let result = expand_pattern("file{1-3");
        match result {
            Err(PatternError::SyntaxError(msg)) => {
                assert!(msg.contains("Unclosed"));
            }
            _ => panic!("expected SyntaxError"),
        }
    }

    // Numeric edge cases
    #[test]
    fn test_expand_u64_max_range_single_value() {
        // A range where start == end at large values
        let urls = expand_pattern("file{999999999-999999999}.txt").unwrap();
        assert_eq!(urls, vec!["file999999999.txt"]);
    }

    #[test]
    fn test_expand_numeric_padding_width_4() {
        let urls = expand_pattern("file{0001-0003}.txt").unwrap();
        assert_eq!(
            urls,
            vec!["file0001.txt", "file0002.txt", "file0003.txt"]
        );
    }

    #[test]
    fn test_expand_numeric_no_padding_single_digit() {
        let urls = expand_pattern("file{1-3}.txt").unwrap();
        // No padding: "1", "2", "3"
        assert_eq!(urls, vec!["file1.txt", "file2.txt", "file3.txt"]);
    }

    // Alpha edge cases
    #[test]
    fn test_expand_alpha_b_to_d() {
        let urls = expand_pattern("{b-d}").unwrap();
        assert_eq!(urls, vec!["b", "c", "d"]);
    }

    #[test]
    fn test_expand_alpha_uppercase_with_step_2() {
        let urls = expand_pattern("{A-E:2}").unwrap();
        assert_eq!(urls, vec!["A", "C", "E"]);
    }

    // Integration: complex real-world patterns
    #[test]
    fn test_expand_image_sequence_pattern() {
        let urls = expand_pattern("http://cdn.example.com/photos/2024/img_{001-005}.jpg").unwrap();
        assert_eq!(urls.len(), 5);
        assert_eq!(
            urls[0],
            "http://cdn.example.com/photos/2024/img_001.jpg"
        );
    }

    #[test]
    fn test_expand_log_file_pattern() {
        let urls = expand_pattern("http://logs.example.com/2024/log_{01-12}.txt").unwrap();
        assert_eq!(urls.len(), 12);
        assert_eq!(urls[0], "http://logs.example.com/2024/log_01.txt");
        assert_eq!(urls[11], "http://logs.example.com/2024/log_12.txt");
    }

    #[test]
    fn test_expand_alphabetic_index_pattern() {
        let urls = expand_pattern("http://dict.example.com/{a-f}/words.txt").unwrap();
        assert_eq!(urls.len(), 6);
        assert_eq!(urls[0], "http://dict.example.com/a/words.txt");
        assert_eq!(urls[5], "http://dict.example.com/f/words.txt");
    }
}
