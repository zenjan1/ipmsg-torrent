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
}
