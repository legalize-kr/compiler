//! Renders parsed precedent data into Markdown bytes and commit messages.

use std::sync::OnceLock;

use anyhow::Result;
use regex::Regex;
use rustc_hash::FxHashMap as HashMap;
use serde::Serialize;

use crate::git_repo::RepoPathBuf;
use crate::xml_parser::{PrecedentDetail, PrecedentMetadata};

/// Tracks already-assigned output paths so collisions follow the legacy rules.
#[derive(Debug, Default)]
pub struct PathRegistry {
    /// Already assigned paths keyed by the rendered repository path string.
    assigned: HashMap<String, String>,
}

/// Returns the Markdown path for one parsed precedent, registering collisions.
pub fn get_precedent_path(
    metadata: &PrecedentMetadata,
    registry: &mut PathRegistry,
) -> RepoPathBuf {
    let case_type = normalize_case_type(&metadata.case_type_raw);
    let court_tier = court_tier_label(&metadata.court_code);
    let raw_case_no = metadata.case_no.trim();
    let filename = if raw_case_no.is_empty() {
        metadata.serial.clone()
    } else {
        cap_filename_bytes(&sanitize_case_number(raw_case_no), &metadata.serial)
    };

    let base_filename = format!("{filename}.md");
    let base_path = RepoPathBuf::prec_file(&case_type, &court_tier, &base_filename);
    let base_key = base_path.to_string();

    let final_path = match registry.assigned.get(&base_key) {
        Some(existing) if existing != &metadata.serial => {
            let qualified_filename = format!("{filename}_{}.md", metadata.serial);
            RepoPathBuf::prec_file(&case_type, &court_tier, &qualified_filename)
        }
        _ => base_path,
    };

    registry
        .assigned
        .insert(final_path.to_string(), metadata.serial.clone());
    final_path
}

/// Court abbreviation expansion patterns kept in priority order.
fn court_abbrev_patterns() -> &'static [(Regex, &'static str)] {
    static INSTANCE: OnceLock<Vec<(Regex, &'static str)>> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        vec![
            (Regex::new(r"고법$").unwrap(), "고등법원"),
            (Regex::new(r"지법$").unwrap(), "지방법원"),
            (Regex::new(r"행법$").unwrap(), "행정법원"),
        ]
    })
}

/// Expands common court abbreviations such as `서울고법` -> `서울고등법원`.
pub fn normalize_court_name(name: &str) -> String {
    let mut current = name.to_owned();
    for (pattern, replacement) in court_abbrev_patterns() {
        current = pattern.replace(&current, *replacement).into_owned();
    }
    current
}

/// Maps a 법원종류코드 to the display tier label (`대법원` / `하급심` / `미분류`).
pub fn court_tier_label(court_code: &str) -> String {
    match court_code {
        "400201" => String::from("대법원"),
        "400202" => String::from("하급심"),
        _ => String::from("미분류"),
    }
}

/// Normalizes the raw 사건종류명 field according to the legacy renderer rules.
pub fn normalize_case_type(case_type: &str) -> String {
    if case_type.is_empty() {
        return String::from("기타");
    }
    if case_type.contains(',') {
        return case_type.replace(", ", "·").replace(',', "·");
    }
    match case_type {
        "민사" | "형사" | "일반행정" | "세무" | "특허" | "가사" => {
            case_type.to_owned()
        }
        _ => String::from("기타"),
    }
}

/// Pattern that strips a leading parenthesised court-location prefix such as `(창원)`.
fn leading_parens_re() -> &'static Regex {
    static INSTANCE: OnceLock<Regex> = OnceLock::new();
    INSTANCE.get_or_init(|| Regex::new(r"^\([^)]+\)").unwrap())
}

/// Pattern that converts trailing `(참가)` style suffixes into `_참가`.
fn remaining_parens_re() -> &'static Regex {
    static INSTANCE: OnceLock<Regex> = OnceLock::new();
    INSTANCE.get_or_init(|| Regex::new(r"\(([^)]+)\)").unwrap())
}

/// Sanitizes a 사건번호 value so it can be used as the leaf filename.
pub fn sanitize_case_number(case_no: &str) -> String {
    let trimmed = case_no.trim();
    let stripped_leading = leading_parens_re().replace(trimmed, "").into_owned();
    let comma_normalized = stripped_leading.replace(", ", "_").replace(',', "_");
    remaining_parens_re()
        .replace_all(&comma_normalized, "_$1")
        .into_owned()
}

/// Maximum byte length for a filename stem (leaves headroom for `.md` and the
/// collision `_{serial}` suffix within the 255-byte `NAME_MAX` limit on APFS).
const MAX_FILENAME_STEM_BYTES: usize = 180;

/// Caps a filename stem to `MAX_FILENAME_STEM_BYTES` bytes, appending
/// `_{serial}` when truncation occurs so the resulting path stays unique and
/// traceable back to the source precedent.
pub fn cap_filename_bytes(filename: &str, serial: &str) -> String {
    if filename.len() <= MAX_FILENAME_STEM_BYTES {
        return filename.to_owned();
    }
    let suffix = format!("_{serial}");
    let keep = MAX_FILENAME_STEM_BYTES.saturating_sub(suffix.len());
    let mut end = keep;
    while end > 0 && !filename.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &filename[..end], suffix)
}

/// Pattern matching `<br>` and `<br/>` tags during HTML stripping.
fn br_re() -> &'static Regex {
    static INSTANCE: OnceLock<Regex> = OnceLock::new();
    INSTANCE.get_or_init(|| Regex::new(r"(?i)<br\s*/?>").unwrap())
}

/// Pattern matching any remaining HTML tag during HTML stripping.
fn html_tag_re() -> &'static Regex {
    static INSTANCE: OnceLock<Regex> = OnceLock::new();
    INSTANCE.get_or_init(|| Regex::new(r"<[^>]+>").unwrap())
}

/// Pattern matching three or more consecutive newlines for blank-line collapsing.
fn multi_blank_re() -> &'static Regex {
    static INSTANCE: OnceLock<Regex> = OnceLock::new();
    INSTANCE.get_or_init(|| Regex::new(r"\n{3,}").unwrap())
}

/// Pattern matching three or more consecutive spaces/non-breaking spaces for collapsing.
fn multi_space_re() -> &'static Regex {
    static INSTANCE: OnceLock<Regex> = OnceLock::new();
    INSTANCE.get_or_init(|| Regex::new(r"[ \u{00A0}]{3,}").unwrap())
}

/// Inline whitespace normalization for 사건명 (frontmatter + H1 title).
///
/// Converts `<br>` to a single space (keeps the name single-line), strips
/// remaining tags, decodes HTML entities, and collapses 3+ space/NBSP runs.
pub fn normalize_case_name(text: &str) -> String {
    let with_spaces = br_re().replace_all(text, " ").into_owned();
    let stripped = html_tag_re().replace_all(&with_spaces, "").into_owned();
    let decoded = stripped
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ");
    let spaced = multi_space_re().replace_all(&decoded, " ").into_owned();
    spaced.trim().to_owned()
}

/// Converts an HTML-bearing precedent section into plain Markdown text.
pub fn html_to_markdown(html: &str) -> String {
    let with_newlines = br_re().replace_all(html, "\n").into_owned();
    let stripped = html_tag_re().replace_all(&with_newlines, "").into_owned();
    let decoded = stripped
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ");
    let collapsed = multi_blank_re().replace_all(&decoded, "\n\n").into_owned();
    let spaced = multi_space_re().replace_all(&collapsed, " ").into_owned();
    spaced.trim().to_owned()
}

/// Converts a `YYYYMMDD` 선고일자 to `YYYY-MM-DD`, returning `None` for sentinel values.
pub fn format_judgment_date(date_str: &str) -> Option<String> {
    if date_str.len() != 8 || !date_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let year_prefix = &date_str[..4];
    if year_prefix == "0000" || year_prefix == "0001" {
        return None;
    }
    Some(format!(
        "{}-{}-{}",
        &date_str[..4],
        &date_str[4..6],
        &date_str[6..8]
    ))
}

/// Renders one parsed precedent document into the repository Markdown format.
pub fn precedent_to_markdown(detail: &PrecedentDetail) -> Result<Vec<u8>> {
    let case_name = normalize_case_name(&detail.metadata.case_name);
    let frontmatter = Frontmatter {
        serial: &detail.metadata.serial,
        case_no: &detail.metadata.case_no,
        case_name: &case_name,
        court_name: normalize_court_name(&detail.metadata.court_name),
        court_tier: court_tier_label(&detail.metadata.court_code),
        case_type: normalize_case_type(&detail.metadata.case_type_raw),
        source: format!("https://www.law.go.kr/판례/{}", detail.metadata.serial),
        judgment_date: format_judgment_date(&detail.metadata.judgment_date),
    };
    let mut yaml = serde_yaml::to_string(&frontmatter)?;
    if let Some(stripped) = yaml.strip_prefix("---\n") {
        yaml = stripped.to_owned();
    }

    let title = if !case_name.is_empty() {
        case_name.as_str()
    } else if !detail.metadata.case_no.is_empty() {
        detail.metadata.case_no.as_str()
    } else {
        detail.metadata.serial.as_str()
    };

    let mut body_parts = vec![format!("# {title}"), String::new()];
    let sections: [(&str, &str); 5] = [
        ("판시사항", &detail.body.ruling_matters),
        ("판결요지", &detail.body.ruling_summary),
        ("참조조문", &detail.body.referenced_laws),
        ("참조판례", &detail.body.referenced_cases),
        ("판례내용", &detail.body.full_text),
    ];
    for (heading, content) in sections {
        let trimmed = content.trim();
        if trimmed.is_empty() {
            continue;
        }
        let rendered = html_to_markdown(trimmed);
        if rendered.is_empty() {
            continue;
        }
        body_parts.push(format!("## {heading}"));
        body_parts.push(String::new());
        body_parts.push(rendered);
        body_parts.push(String::new());
    }

    let body = body_parts.join("\n");
    Ok(format!("---\n{yaml}---\n\n{body}\n").into_bytes())
}

/// Builds the Git commit message for one precedent revision.
pub fn build_commit_message(metadata: &PrecedentMetadata) -> String {
    let title = if !metadata.case_name.is_empty() {
        format!("판례: {}", metadata.case_name)
    } else {
        format!("판례: {}", metadata.case_no)
    };
    let date_line = format_judgment_date(&metadata.judgment_date).unwrap_or_default();
    let mut lines = Vec::with_capacity(7);
    lines.push(title);
    lines.push(String::new());
    lines.push(format!(
        "판례: https://www.law.go.kr/판례/({})",
        metadata.serial
    ));
    lines.push(format!("선고일자: {date_line}"));
    lines.push(format!("법원명: {}", metadata.court_name));
    lines.push(format!("사건종류: {}", metadata.case_type_raw));
    lines.push(format!("판례일련번호: {}", metadata.serial));
    lines.join("\n")
}

/// YAML frontmatter payload for one rendered Markdown file.
#[derive(Debug, Serialize)]
struct Frontmatter<'a> {
    /// 판례일련번호.
    #[serde(rename = "판례일련번호")]
    serial: &'a str,
    /// 사건번호.
    #[serde(rename = "사건번호")]
    case_no: &'a str,
    /// 사건명.
    #[serde(rename = "사건명")]
    case_name: &'a str,
    /// 법원명 (정규화).
    #[serde(rename = "법원명")]
    court_name: String,
    /// 법원등급 (`대법원` / `하급심` / `미분류`).
    #[serde(rename = "법원등급")]
    court_tier: String,
    /// 사건종류 (정규화).
    #[serde(rename = "사건종류")]
    case_type: String,
    /// 출처 URL.
    #[serde(rename = "출처")]
    source: String,
    /// 선고일자 (`YYYY-MM-DD`), omitted when missing.
    #[serde(rename = "선고일자", skip_serializing_if = "Option::is_none")]
    judgment_date: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xml_parser::PrecedentBody;

    #[test]
    fn normalize_case_type_handles_known_types_and_fallbacks() {
        assert_eq!(normalize_case_type(""), "기타");
        assert_eq!(normalize_case_type("민사"), "민사");
        assert_eq!(normalize_case_type("형사"), "형사");
        assert_eq!(normalize_case_type("미정의"), "기타");
        assert_eq!(normalize_case_type("선거,특별"), "선거·특별");
        assert_eq!(normalize_case_type("선거, 특별"), "선거·특별");
    }

    #[test]
    fn court_abbreviations_expand_to_full_names() {
        assert_eq!(normalize_court_name("서울고법"), "서울고등법원");
        assert_eq!(normalize_court_name("서울지법"), "서울지방법원");
        assert_eq!(normalize_court_name("서울행법"), "서울행정법원");
        assert_eq!(normalize_court_name("대법원"), "대법원");
    }

    #[test]
    fn sanitizes_case_numbers() {
        assert_eq!(sanitize_case_number("(창원)2024가합1234"), "2024가합1234");
        assert_eq!(sanitize_case_number("2000므1257, 1264"), "2000므1257_1264");
        assert_eq!(
            sanitize_case_number("2000므1257(본소), 1264(반소)"),
            "2000므1257_본소_1264_반소"
        );
    }

    #[test]
    fn collisions_get_serial_suffix() {
        let mut registry = PathRegistry::default();
        let first = get_precedent_path(
            &PrecedentMetadata {
                serial: String::from("100"),
                case_no: String::from("2024가합1"),
                court_code: String::from("400201"),
                case_type_raw: String::from("민사"),
                ..PrecedentMetadata::default()
            },
            &mut registry,
        );
        let second = get_precedent_path(
            &PrecedentMetadata {
                serial: String::from("200"),
                case_no: String::from("2024가합1"),
                court_code: String::from("400201"),
                case_type_raw: String::from("민사"),
                ..PrecedentMetadata::default()
            },
            &mut registry,
        );
        assert_eq!(first.to_string(), "민사/대법원/2024가합1.md");
        assert_eq!(second.to_string(), "민사/대법원/2024가합1_200.md");
    }

    #[test]
    fn long_merged_case_numbers_are_capped_within_name_max() {
        let many_numbers: Vec<String> = (700..1000).map(|n| n.to_string()).collect();
        let long_case = format!("2011고합669, {} (병합) (분리)", many_numbers.join(", "));
        let mut registry = PathRegistry::default();
        let path = get_precedent_path(
            &PrecedentMetadata {
                serial: String::from("123456"),
                case_no: long_case,
                court_code: String::from("400202"),
                case_type_raw: String::from("형사"),
                ..PrecedentMetadata::default()
            },
            &mut registry,
        );
        let path_str = path.to_string();
        let leaf = path_str.rsplit('/').next().unwrap();
        assert!(
            leaf.len() <= 200,
            "leaf filename must fit NAME_MAX headroom: {} bytes -> {}",
            leaf.len(),
            leaf
        );
        assert!(
            leaf.ends_with("_123456.md"),
            "expected serial suffix for truncated filename: {leaf}"
        );
    }

    #[test]
    fn short_case_numbers_are_not_modified_by_cap() {
        assert_eq!(cap_filename_bytes("2024가합1", "100"), "2024가합1");
    }

    #[test]
    fn format_judgment_date_rejects_sentinels() {
        assert_eq!(
            format_judgment_date("20240101").as_deref(),
            Some("2024-01-01")
        );
        assert_eq!(format_judgment_date(""), None);
        assert_eq!(format_judgment_date("00000101"), None);
        assert_eq!(format_judgment_date("0001-01"), None);
    }

    #[test]
    fn renders_markdown_with_sections() {
        let detail = PrecedentDetail {
            metadata: PrecedentMetadata {
                serial: String::from("145683"),
                case_no: String::from("2000므1257(본소), 1264(반소)"),
                case_name: String::from("손해배상"),
                court_name: String::from("대법원"),
                court_code: String::from("400201"),
                judgment_date: String::from("20031114"),
                case_type_raw: String::from("가사"),
            },
            body: PrecedentBody {
                ruling_matters: String::from("<br/>판시 본문<br/>"),
                ruling_summary: String::from("요지 본문"),
                referenced_laws: String::new(),
                referenced_cases: String::new(),
                full_text: String::from("<p>전문</p>"),
            },
        };
        let markdown = String::from_utf8(precedent_to_markdown(&detail).unwrap()).unwrap();
        assert!(markdown.contains("판례일련번호: '145683'"));
        assert!(markdown.contains("법원등급: 대법원"));
        assert!(markdown.contains("2003-11-14"));
        assert!(markdown.contains("# 손해배상"));
        assert!(markdown.contains("## 판시사항"));
        assert!(markdown.contains("판시 본문"));
        assert!(!markdown.contains("## 참조조문"));
    }

    #[test]
    fn multi_space_collapses_three_or_more_spaces() {
        assert_eq!(html_to_markdown("a   b"), "a b");
        assert_eq!(html_to_markdown("a  b"), "a  b");
        assert_eq!(html_to_markdown("a\u{00A0}\u{00A0}\u{00A0}b"), "a b");
        assert_eq!(html_to_markdown("a     b\nc   d"), "a b\nc d");
    }

    #[test]
    fn nbsp_decoded_then_space_collapsed() {
        assert_eq!(html_to_markdown("a&nbsp;&nbsp;&nbsp;b"), "a b");
    }
}
