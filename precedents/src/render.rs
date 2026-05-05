//! Renders parsed precedent data into Markdown bytes and commit messages.

use std::sync::OnceLock;

use anyhow::Result;
use regex::Regex;
use rustc_hash::FxHashMap as HashMap;
use serde::Serialize;
use time::{Date, Month};
use unicode_normalization::UnicodeNormalization;

use crate::xml_parser::{
    MISSING_COURT_SENTINEL, MISSING_DATE_SENTINEL, PrecedentDetail, PrecedentMetadata,
};
use git_writer::RepoPathBuf;

/// Slot separator for the composite filename grammar `{COURT}{SEP}{DATE}{SEP}{CASENO}`.
///
/// Single underscore chosen for readability. Note that `sanitize_case_number` can
/// also emit `_` inside the CASENO slot (merged cases like `2000나10828_10835_병합`).
/// Parsing is therefore left-anchored with `splitn(3, SEP)` — court names are Korean
/// and never contain `_`, the date is fixed `YYYY-MM-DD`, so the first two splits
/// always isolate (court, date) and the remainder is CASENO.
///
/// Kept as a single source of truth so the SEP swap is a one-line change, and must stay
/// in lockstep with the Python (`legalize-pipeline`) and cli-tools sides.
pub const SEP: &str = "_";

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
    let stem = compose_filename_stem(
        &metadata.court_name,
        &metadata.judgment_date,
        &metadata.case_no,
        &metadata.serial,
    );

    let base_filename = format!("{stem}.md");
    let base_path = RepoPathBuf::prec_file(&case_type, &court_tier, &base_filename);
    let base_key = base_path.to_string();

    let final_path = match registry.assigned.get(&base_key) {
        Some(existing) if existing != &metadata.serial => {
            let qualified_filename = format!("{stem}_{}.md", metadata.serial);
            RepoPathBuf::prec_file(&case_type, &court_tier, &qualified_filename)
        }
        _ => base_path,
    };

    registry
        .assigned
        .insert(final_path.to_string(), metadata.serial.clone());
    final_path
}

/// Composes the filename stem `{COURT}{SEP}{DATE}{SEP}{CASENO}` for one precedent.
///
/// Mirrors `legalize-pipeline/precedents/converter.py:compose_filename_stem` byte-for-byte.
///
/// Missing-value policy (see plan §1.4):
/// - Empty/sentinel `judgment_date` → `0000-00-00` so the grammar slot survives.
/// - Empty `court_name` → `미상법원` AND CASENO is forced to `serial` (since the
///   composite key would otherwise lose its discriminator).
/// - Empty `case_no` → `serial` fallback (legacy behavior).
///
/// All path components are NFC-normalized after normalization. The final stem is
/// capped to `MAX_FILENAME_STEM_BYTES` via `cap_caseno_slot`, which trims only the
/// CASENO slot so both SEP slots survive.
pub fn compose_filename_stem(
    court_name: &str,
    judgment_date: &str,
    case_no: &str,
    serial: &str,
) -> String {
    let normalized_court = normalize_court_name(court_name.trim());
    let court_nfc: String = normalized_court.nfc().collect();
    let (court, force_serial_caseno) = if court_nfc.is_empty() {
        (MISSING_COURT_SENTINEL.to_owned(), true)
    } else {
        (court_nfc, false)
    };

    let date =
        format_judgment_date(judgment_date).unwrap_or_else(|| MISSING_DATE_SENTINEL.to_owned());

    let caseno_raw = case_no.trim();
    let caseno = if caseno_raw.is_empty() || force_serial_caseno {
        serial.nfc().collect::<String>()
    } else {
        sanitize_case_number(caseno_raw).nfc().collect::<String>()
    };

    cap_caseno_slot(&court, &date, &caseno, serial)
}

/// Caps the composite stem to `MAX_FILENAME_STEM_BYTES`, trimming the CASENO slot only.
///
/// `_{serial}` suffix matches the legacy `cap_filename_bytes` policy so collision
/// resolution stays uniform.
pub fn cap_caseno_slot(court: &str, date: &str, caseno: &str, serial: &str) -> String {
    let stem = format!("{court}{SEP}{date}{SEP}{caseno}");
    if stem.len() <= MAX_FILENAME_STEM_BYTES {
        return stem;
    }
    let prefix = format!("{court}{SEP}{date}{SEP}");
    let suffix = format!("_{serial}");
    let Some(keep) = MAX_FILENAME_STEM_BYTES
        .checked_sub(prefix.len())
        .and_then(|remaining| remaining.checked_sub(suffix.len()))
    else {
        return format!("{prefix}{serial}");
    };
    let mut end = keep.min(caseno.len());
    while end > 0 && !caseno.is_char_boundary(end) {
        end -= 1;
    }
    format!("{prefix}{}{suffix}", &caseno[..end])
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

/// Legacy single-key cap (pre-Phase-1 behavior): caps `filename` to MAX bytes and
/// appends `_{serial}` on truncation. Kept for `legacy_get_precedent_path` so
/// `legacy-paths.json` emission can map old precedent-kr files exactly.
fn legacy_cap_filename_bytes(filename: &str, serial: &str) -> String {
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

/// Legacy single-key path resolver — mirrors the pre-rename `precedent-kr` filenames.
///
/// Used only by Phase 3 `legacy-paths.json` emission to map current files in the
/// `precedent-kr` baseline to their composite-key successors.
pub fn legacy_get_precedent_path(
    metadata: &PrecedentMetadata,
    registry: &mut PathRegistry,
) -> RepoPathBuf {
    let case_type = normalize_case_type(&metadata.case_type_raw);
    let court_tier = court_tier_label(&metadata.court_code);
    let raw_case_no = metadata.case_no.trim();
    let filename = if raw_case_no.is_empty() {
        metadata.serial.clone()
    } else {
        legacy_cap_filename_bytes(&sanitize_case_number(raw_case_no), &metadata.serial)
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

/// Pattern matching decimal and hexadecimal numeric HTML character references.
fn numeric_entity_re() -> &'static Regex {
    static INSTANCE: OnceLock<Regex> = OnceLock::new();
    INSTANCE.get_or_init(|| Regex::new(r"&#(?:x([0-9A-Fa-f]+)|([0-9]+));").unwrap())
}

/// Decodes the HTML entities that appear in upstream text snippets.
fn decode_html_entities(text: &str) -> String {
    let decoded = text
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ");

    numeric_entity_re()
        .replace_all(&decoded, |captures: &regex::Captures<'_>| {
            let parsed = captures
                .get(1)
                .map(|hex| u32::from_str_radix(hex.as_str(), 16))
                .unwrap_or_else(|| captures[2].parse::<u32>());
            parsed
                .ok()
                .and_then(char::from_u32)
                .map(|ch| ch.to_string())
                .unwrap_or_else(|| captures[0].to_owned())
        })
        .into_owned()
}

/// Inline whitespace normalization for 사건명 (frontmatter + H1 title).
///
/// Converts `<br>` to a single space (keeps the name single-line), strips
/// remaining tags, decodes HTML entities, and collapses 3+ space/NBSP runs.
pub fn normalize_case_name(text: &str) -> String {
    let with_spaces = br_re().replace_all(text, " ").into_owned();
    let stripped = html_tag_re().replace_all(&with_spaces, "").into_owned();
    let decoded = decode_html_entities(&stripped);
    let spaced = multi_space_re().replace_all(&decoded, " ").into_owned();
    spaced.trim().to_owned()
}

/// Converts an HTML-bearing precedent section into plain Markdown text.
pub fn html_to_markdown(html: &str) -> String {
    let with_newlines = br_re().replace_all(html, "\n").into_owned();
    let stripped = html_tag_re().replace_all(&with_newlines, "").into_owned();
    let decoded = decode_html_entities(&stripped);
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
    let year = year_prefix.parse::<i32>().ok()?;
    let month = Month::try_from(date_str[4..6].parse::<u8>().ok()?).ok()?;
    let day = date_str[6..8].parse::<u8>().ok()?;
    Date::from_calendar_date(year, month, day).ok()?;
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
        source: format!(
            "https://www.law.go.kr/LSW/precInfoP.do?precSeq={}",
            detail.metadata.serial
        ),
        attachments: Vec::new(),
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
        "판례: https://www.law.go.kr/LSW/precInfoP.do?precSeq={}",
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
    /// Structured attachment links. PrecService currently has no separate attachment fields.
    #[serde(rename = "첨부파일")]
    attachments: Vec<String>,
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
    fn compose_filename_stem_happy_path() {
        let stem = compose_filename_stem("대법원", "20030310", "2002다56116", "100");
        assert_eq!(stem, "대법원_2003-03-10_2002다56116");
    }

    #[test]
    fn compose_filename_stem_handles_merged_case_no() {
        let stem = compose_filename_stem(
            "대법원",
            "20031114",
            "2000므1257(본소), 1264(반소)",
            "145683",
        );
        assert_eq!(stem, "대법원_2003-11-14_2000므1257_본소_1264_반소");
    }

    #[test]
    fn compose_filename_stem_missing_date_uses_sentinel() {
        let stem = compose_filename_stem("대법원", "", "2024가합1", "100");
        assert_eq!(stem, "대법원_0000-00-00_2024가합1");
        let stem = compose_filename_stem("대법원", "00000000", "2024가합1", "100");
        assert_eq!(stem, "대법원_0000-00-00_2024가합1");
    }

    #[test]
    fn compose_filename_stem_missing_court_falls_back_to_serial() {
        // Empty court → "미상법원" + CASENO forced to serial.
        let stem = compose_filename_stem("", "20240101", "2024가합1", "999");
        assert_eq!(stem, "미상법원_2024-01-01_999");
    }

    #[test]
    fn compose_filename_stem_caps_only_caseno_slot() {
        let many: Vec<String> = (700..1000).map(|n| n.to_string()).collect();
        let long_case = format!("2011고합669, {} (병합) (분리)", many.join(", "));
        let stem = compose_filename_stem("대법원", "20110315", &long_case, "123456");
        assert!(
            stem.len() <= MAX_FILENAME_STEM_BYTES,
            "stem must fit MAX_FILENAME_STEM_BYTES: {} bytes",
            stem.len()
        );
        assert!(
            stem.starts_with("대법원_2011-03-15_"),
            "court+date prefix preserved: {stem}"
        );
        assert!(
            stem.ends_with("_123456"),
            "serial suffix appended on truncation: {stem}"
        );
    }

    #[test]
    fn cap_caseno_slot_falls_back_to_serial_when_prefix_exceeds_budget() {
        let long_court = "가".repeat(80);
        let stem = cap_caseno_slot(&long_court, "2024-01-01", "2024가합1", "123456");
        assert_eq!(stem, format!("{long_court}_2024-01-01_123456"));
    }

    #[test]
    fn compose_filename_stem_normalizes_to_nfc() {
        // Decomposed Hangul (NFD): 가 = U+1100 U+1161, expect NFC-composed 가 in output.
        let nfd_court = "\u{1103}\u{1162}\u{1107}\u{1165}\u{11B8}\u{110B}\u{116F}\u{11AB}"; // 대법원
        let stem = compose_filename_stem(nfd_court, "20240101", "2024가합1", "100");
        assert_eq!(stem, "대법원_2024-01-01_2024가합1");
        // Stem is in NFC: per-char count matches NFC form.
        assert_eq!(
            stem.chars().count(),
            "대법원_2024-01-01_2024가합1".chars().count()
        );
    }

    #[test]
    fn collisions_get_serial_suffix() {
        // Composite key collision: same court+date+caseno, different serials.
        let mut registry = PathRegistry::default();
        let first = get_precedent_path(
            &PrecedentMetadata {
                serial: String::from("100"),
                case_no: String::from("2024가합1"),
                court_name: String::from("대법원"),
                court_code: String::from("400201"),
                judgment_date: String::from("20240101"),
                case_type_raw: String::from("민사"),
                ..PrecedentMetadata::default()
            },
            &mut registry,
        );
        let second = get_precedent_path(
            &PrecedentMetadata {
                serial: String::from("200"),
                case_no: String::from("2024가합1"),
                court_name: String::from("대법원"),
                court_code: String::from("400201"),
                judgment_date: String::from("20240101"),
                case_type_raw: String::from("민사"),
                ..PrecedentMetadata::default()
            },
            &mut registry,
        );
        assert_eq!(
            first.to_string(),
            "민사/대법원/대법원_2024-01-01_2024가합1.md"
        );
        assert_eq!(
            second.to_string(),
            "민사/대법원/대법원_2024-01-01_2024가합1_200.md"
        );
    }

    #[test]
    fn sanitize_case_number_emits_underscore_for_merged_cases() {
        // sanitize output legitimately contains `_` (= SEP) for merged cases. Composite
        // filename grammar parses left-anchored with splitn(3, SEP) so the embedded
        // underscores in CASENO never break (court name has no `_`, date is fixed shape).
        assert_eq!(sanitize_case_number("(창원)2024가합1234"), "2024가합1234");
        assert_eq!(sanitize_case_number("2000므1257, 1264"), "2000므1257_1264");
        assert_eq!(
            sanitize_case_number("2000므1257(본소), 1264(반소)"),
            "2000므1257_본소_1264_반소"
        );
        assert!(sanitize_case_number("2011고합669, 700, 701 (병합)").contains('_'));
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
                court_name: String::from("부산지방법원"),
                court_code: String::from("400202"),
                judgment_date: String::from("20110315"),
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
    fn format_judgment_date_rejects_sentinels() {
        assert_eq!(
            format_judgment_date("20240101").as_deref(),
            Some("2024-01-01")
        );
        assert_eq!(
            format_judgment_date("20240229").as_deref(),
            Some("2024-02-29")
        );
        assert_eq!(format_judgment_date(""), None);
        assert_eq!(format_judgment_date("00000101"), None);
        assert_eq!(format_judgment_date("0001-01"), None);
        assert_eq!(format_judgment_date("20230229"), None);
        assert_eq!(format_judgment_date("20241301"), None);
        assert_eq!(format_judgment_date("20240231"), None);
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
        assert!(markdown.contains("첨부파일: []"));
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

    #[test]
    fn numeric_html_entities_are_decoded() {
        assert_eq!(normalize_case_name("손해&#40;배상&#41;"), "손해(배상)");
        assert_eq!(html_to_markdown("A&#x2F;B &#47; C"), "A/B / C");
    }
}
