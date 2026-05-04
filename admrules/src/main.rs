//! Compile cached law.go.kr administrative-rule XML into a Markdown tree.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use git_writer::{BareRepoWriter, GitTimestampKst, RepoPathBuf, precompute_blob};
use quick_xml::Reader;
use quick_xml::events::Event;
use time::{Date, Month, PrimitiveDateTime, Time as CivilTime, UtcOffset};
use unicode_normalization::UnicodeNormalization;

const REPOSITORY_README: &[u8] = include_bytes!("../assets/README.md");

/// Command-line interface.
#[derive(Debug, Parser)]
#[command(name = "admrule-kr-compiler")]
struct Cli {
    /// Directory containing `{행정규칙일련번호}.xml` files.
    cache_dir: PathBuf,
    /// Output Markdown tree directory.
    #[arg(short = 'o', long = "output")]
    output: PathBuf,
    /// Limit input files for probe runs.
    #[arg(long)]
    limit: Option<usize>,
    /// Write a bare Git repository instead of a Markdown tree.
    #[arg(long)]
    bare: bool,
}

/// Parsed administrative-rule metadata and body.
#[derive(Debug, Clone)]
struct Admrule {
    /// 행정규칙일련번호.
    serial: String,
    /// 행정규칙ID.
    rule_id: String,
    /// 행정규칙명.
    name: String,
    /// 행정규칙종류.
    rule_type: String,
    /// Canonical top-level agency for repository grouping.
    top_ministry: String,
    /// 소관부처명.
    ministry: String,
    /// Original 소관부처명 before path-stability normalization.
    original_ministry: String,
    /// 기관코드.
    org_code: String,
    /// 발령번호.
    issue_no: String,
    /// 발령일자 raw.
    issue_date_raw: String,
    /// 시행일자 raw.
    effective_date_raw: String,
    /// 제개정구분.
    amendment: String,
    /// 제개정구분코드.
    amendment_code: String,
    /// 현행연혁구분.
    current_history: String,
    /// Body text.
    body: String,
}

/// 2026-03-30 12:00:00 KST (UTC+9) = 2026-03-30 03:00:00 UTC.
const INITIAL_COMMIT_EPOCH: i64 = 1_774_839_600;

/// Entry point.
fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.bare {
        compile_bare_repo(&cli.cache_dir, &cli.output, cli.limit)
    } else {
        compile_dir(&cli.cache_dir, &cli.output, cli.limit)
    }
}

/// Compile XML directly into a bare Git repository.
fn compile_bare_repo(cache_dir: &Path, output: &Path, limit: Option<usize>) -> Result<()> {
    let entries = render_admrule_entries(cache_dir, limit)?;
    if entries.is_empty() {
        anyhow::bail!(
            "no valid admrule XML files found under {}",
            cache_dir.display()
        );
    }

    let mut repo = BareRepoWriter::create(output)?;
    repo.commit_static(
        &RepoPathBuf::root_file("README.md"),
        REPOSITORY_README,
        "initial commit",
        INITIAL_COMMIT_EPOCH,
    )?;
    for entry in &entries {
        let (blob_sha, compressed_blob) = precompute_blob(&entry.content);
        repo.commit_bot_file(
            &RepoPathBuf::file(&entry.path),
            &entry.content,
            blob_sha,
            &compressed_blob,
            &entry.message,
            GitTimestampKst::from_epoch(entry.timestamp),
        )?;
    }
    repo.finish()?;
    eprintln!("committed {} admrule markdown files", entries.len());
    Ok(())
}

#[derive(Debug)]
struct ImportEntry {
    path: String,
    content: Vec<u8>,
    message: String,
    timestamp: i64,
    sort_date: String,
    sort_id: u64,
}

fn render_admrule_entries(cache_dir: &Path, limit: Option<usize>) -> Result<Vec<ImportEntry>> {
    let mut files = read_xml_files(cache_dir)?;
    if let Some(limit) = limit {
        files.truncate(limit);
    }
    let mut registry = BTreeSet::new();
    let mut entries = Vec::with_capacity(files.len());
    for path in files {
        let raw = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let rule = parse_admrule(
            &raw,
            path.file_stem().and_then(|s| s.to_str()).unwrap_or(""),
        )?;
        let rel = admrule_path(&rule, &mut registry);
        entries.push(ImportEntry {
            path: rel.to_string_lossy().replace('\\', "/"),
            content: render_markdown(&rule).into_bytes(),
            message: admrule_commit_message(&rule),
            timestamp: commit_timestamp(&rule.issue_date_raw)?,
            sort_date: compact_date_or_epoch(&rule.issue_date_raw),
            sort_id: rule.serial.parse::<u64>().unwrap_or(u64::MAX),
        });
    }
    entries.sort_by(|a, b| {
        a.sort_date
            .cmp(&b.sort_date)
            .then_with(|| a.sort_id.cmp(&b.sort_id))
            .then_with(|| a.path.cmp(&b.path))
    });
    Ok(entries)
}

fn admrule_commit_message(rule: &Admrule) -> String {
    format!(
        "{}: {} ({})\n\n행정규칙일련번호: {}\n행정규칙ID: {}",
        if rule.rule_type.is_empty() {
            "행정규칙"
        } else {
            &rule.rule_type
        },
        rule.name,
        rule.issue_no,
        rule.serial,
        rule.rule_id
    )
}

fn compact_date_or_epoch(raw: &str) -> String {
    let digits = raw.replace(['.', '-'], "");
    if digits.len() == 8 && digits.bytes().all(|byte| byte.is_ascii_digit()) {
        if digits.as_str() < "19700101" {
            "19700101".to_string()
        } else {
            digits
        }
    } else {
        "19700101".to_string()
    }
}

fn commit_timestamp(raw: &str) -> Result<i64> {
    let date = compact_date_or_epoch(raw);
    let year = date[0..4].parse::<i32>()?;
    let month = Month::try_from(date[4..6].parse::<u8>()?)?;
    let day = date[6..8].parse::<u8>()?;
    let date = Date::from_calendar_date(year, month, day)?;
    let datetime = PrimitiveDateTime::new(date, CivilTime::from_hms(12, 0, 0)?);
    Ok(datetime
        .assume_offset(UtcOffset::from_hms(9, 0, 0)?)
        .unix_timestamp())
}

/// Compile every XML file under `cache_dir` into `output`.
fn compile_dir(cache_dir: &Path, output: &Path, limit: Option<usize>) -> Result<()> {
    fs::create_dir_all(output).with_context(|| format!("failed to create {}", output.display()))?;
    fs::write(output.join("README.md"), REPOSITORY_README)?;
    let mut files = read_xml_files(cache_dir)?;
    if let Some(limit) = limit {
        files.truncate(limit);
    }
    let mut registry = BTreeSet::new();
    let mut written = 0usize;
    for path in files {
        let raw = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let rule = parse_admrule(
            &raw,
            path.file_stem().and_then(|s| s.to_str()).unwrap_or(""),
        )?;
        let rel = admrule_path(&rule, &mut registry);
        let target = output.join(rel);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&target, render_markdown(&rule))?;
        written += 1;
    }
    eprintln!("written {written} admrule markdown files");
    Ok(())
}

/// Return sorted XML files from a flat cache directory.
fn read_xml_files(cache_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(cache_dir)
        .with_context(|| format!("failed to read {}", cache_dir.display()))?
    {
        let path = entry?.path();
        if path.extension().and_then(|s| s.to_str()) == Some("xml") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

/// Parse a cached XML document with a flat tag text map.
fn parse_admrule(raw: &[u8], fallback_serial: &str) -> Result<Admrule> {
    let fields = tag_texts(raw)?;
    let serial = first(&fields, &["행정규칙일련번호", "ID"])
        .unwrap_or(fallback_serial)
        .to_string();
    let body = collect_body(&fields, &["조문내용", "본문", "내용"]);
    let raw_ministry = nfc(first(&fields, &["소관부처명"]).unwrap_or(""));
    let raw_parent = nfc(first(&fields, &["상위부처명"]).unwrap_or(""));
    let (top_ministry, ministry) = resolve_ministry_names(&raw_ministry, &raw_parent);
    let original_ministry = if raw_ministry.is_empty() || raw_ministry == ministry {
        String::new()
    } else {
        raw_ministry
    };
    Ok(Admrule {
        serial,
        rule_id: first(&fields, &["행정규칙ID"]).unwrap_or("").to_string(),
        name: nfc(first(&fields, &["행정규칙명", "행정규칙명_한글"]).unwrap_or("")),
        rule_type: nfc(first(&fields, &["행정규칙종류", "행정규칙종류명"]).unwrap_or("")),
        top_ministry,
        ministry,
        original_ministry,
        org_code: first(&fields, &["기관코드", "소관부처코드"])
            .unwrap_or("")
            .to_string(),
        issue_no: first(&fields, &["발령번호"]).unwrap_or("").to_string(),
        issue_date_raw: first(&fields, &["발령일자"]).unwrap_or("").to_string(),
        effective_date_raw: first(&fields, &["시행일자"]).unwrap_or("").to_string(),
        amendment: first(&fields, &["제개정구분명", "제개정구분"])
            .unwrap_or("")
            .to_string(),
        amendment_code: first(&fields, &["제개정구분코드"])
            .unwrap_or("")
            .to_string(),
        current_history: first(&fields, &["현행연혁구분"]).unwrap_or("").to_string(),
        body,
    })
}

/// Extract all text values by tag name.
fn tag_texts(raw: &[u8]) -> Result<BTreeMap<String, Vec<String>>> {
    let mut reader = Reader::from_reader(raw);
    reader.config_mut().trim_text(true);
    let mut current = String::new();
    let mut fields: BTreeMap<String, Vec<String>> = BTreeMap::new();
    loop {
        match reader.read_event()? {
            Event::Start(event) => {
                current = String::from_utf8_lossy(event.name().as_ref()).to_string()
            }
            Event::Text(text) if !current.is_empty() => {
                let value = text.decode()?.trim().to_string();
                if !value.is_empty() {
                    fields.entry(current.clone()).or_default().push(value);
                }
            }
            Event::CData(text) if !current.is_empty() => {
                let value = text.decode()?.trim().to_string();
                if !value.is_empty() {
                    fields.entry(current.clone()).or_default().push(value);
                }
            }
            Event::End(_) => current.clear(),
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(fields)
}

/// Return first available field value.
fn first<'a>(fields: &'a BTreeMap<String, Vec<String>>, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| fields.get(*key).and_then(|v| v.first().map(String::as_str)))
}

/// Collect body-like fields.
fn collect_body(fields: &BTreeMap<String, Vec<String>>, keys: &[&str]) -> String {
    let mut parts = Vec::new();
    for key in keys {
        if let Some(values) = fields.get(*key) {
            parts.extend(values.iter().map(|v| nfc(v)));
        }
    }
    parts.join("\n\n")
}

/// NFC-normalize a string.
fn nfc(value: &str) -> String {
    value.nfc().collect::<String>()
}

/// Normalize observed ministry-name drift before paths/frontmatter are emitted.
fn normalize_ministry_name(value: &str, fallback: &str) -> String {
    let mut text = nfc(value).trim().to_string();
    if is_iso_date(&text) {
        text = nfc(fallback).trim().to_string();
    }
    let text = text
        .replace("10.29이태원", "10·29이태원")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    canonical_ministry_name(&text).unwrap_or(&text).to_string()
}

fn canonical_ministry_name(value: &str) -> Option<&'static str> {
    match value {
        "문화재청" | "문화재청(구)" => Some("국가유산청"),
        "통계청" => Some("국가데이터처"),
        "특허청" => Some("지식재산처"),
        "환경부" | "환경부(구)" => Some("기후에너지환경부"),
        "산업통상자원부" => Some("산업통상부"),
        "미래창조과학부" => Some("과학기술정보통신부"),
        "중소기업청" => Some("중소벤처기업부"),
        "국가보훈처" => Some("국가보훈부"),
        "방송통신위원회" => Some("방송미디어통신위원회"),
        "여성가족부" => Some("성평등가족부"),
        "식품의약품안전청" => Some("식품의약품안전처"),
        _ => None,
    }
}

fn resolve_ministry_names(ministry: &str, parent: &str) -> (String, String) {
    let mut agency = normalize_ministry_name(ministry, parent);
    let mut top = if parent.trim().is_empty() {
        agency.clone()
    } else {
        normalize_ministry_name(parent, "")
    };
    if top.is_empty() {
        top.clone_from(&agency);
    }
    if agency.is_empty() {
        agency.clone_from(&top);
    }
    (top, agency)
}

fn is_iso_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(idx, byte)| idx == 4 || idx == 7 || byte.is_ascii_digit())
}

/// Safe path component compatible with the Python pipeline.
fn safe_path_part(value: &str) -> String {
    let mut text = nfc(value)
        .replace(['\\', '/', ':', '\0', '"', '\'', '<', '>'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    while !text.is_empty() && text.len() > 180 {
        text.pop();
    }
    if text.is_empty() {
        "_".to_string()
    } else {
        text
    }
}

/// Compute repository path with collision suffixing.
fn admrule_path(rule: &Admrule, registry: &mut BTreeSet<String>) -> PathBuf {
    let ministry = safe_path_part(&rule.top_ministry);
    let issuer = if rule.ministry == rule.top_ministry {
        "_본부".to_string()
    } else {
        safe_path_part(&rule.ministry)
    };
    let rule_type = safe_path_part(&rule.rule_type);
    let name = safe_path_part(&rule.name);
    let base = format!("{ministry}/{issuer}/{rule_type}/{name}/본문.md");
    if registry.insert(base.clone()) {
        return PathBuf::from(base);
    }
    let first_suffix = if rule.issue_no.is_empty() {
        safe_path_part(&rule.serial)
    } else {
        safe_path_part(&rule.issue_no)
    };
    let candidates = [
        first_suffix,
        safe_path_part(&rule.serial),
        safe_path_part(&format!("{}_{}", rule.issue_no, rule.serial)),
    ];
    for suffix in candidates {
        let suffixed = format!("{ministry}/{issuer}/{rule_type}/{name}_{suffix}/본문.md");
        if registry.insert(suffixed.clone()) {
            return PathBuf::from(suffixed);
        }
    }
    let mut idx = 2usize;
    loop {
        let suffixed = format!(
            "{ministry}/{issuer}/{rule_type}/{name}_{}_{idx}/본문.md",
            rule.serial
        );
        if registry.insert(suffixed.clone()) {
            return PathBuf::from(suffixed);
        }
        idx += 1;
    }
}

/// Convert compact dates to ISO dates.
fn format_date(raw: &str) -> String {
    let digits = raw.replace(['.', '-'], "");
    if digits.len() == 8 && digits.chars().all(|c| c.is_ascii_digit()) {
        format!("{}-{}-{}", &digits[..4], &digits[4..6], &digits[6..8])
    } else {
        raw.to_string()
    }
}

/// Clamp pre-epoch dates the same way as the Python pipeline.
fn issue_date(raw: &str) -> (String, bool) {
    let formatted = format_date(raw);
    if formatted.len() == 10 && formatted.as_str() < "1970-01-01" {
        ("1970-01-01".to_string(), true)
    } else {
        (formatted, false)
    }
}

/// Render Markdown.
fn render_markdown(rule: &Admrule) -> String {
    let (issue_date, epoch_clamped) = issue_date(&rule.issue_date_raw);
    let body_source = if rule.body.trim().is_empty() {
        "parsing-failed"
    } else {
        "api-text"
    };
    let body = if rule.body.trim().is_empty() {
        "본문은 국가법령정보센터 원문 또는 첨부파일을 참조하세요.".to_string()
    } else {
        rule.body.trim().to_string()
    };
    let original_ministry = if rule.original_ministry.is_empty() {
        String::new()
    } else {
        format!(
            "소관부처명_원문: {}\n",
            yaml_string(&rule.original_ministry)
        )
    };
    format!(
        "---\n행정규칙ID: {}\n행정규칙일련번호: {}\n행정규칙명: {}\n행정규칙종류: {}\n상위기관명: {}\n소관부처명: {}\n{}기관코드: {}\n발령번호: {}\n발령일자: {}\n시행일자: {}\n제개정구분: {}\n제개정구분코드: {}\n현행연혁구분: {}\nbody_source: {}\nhwp_sha256: null\nattachments_hwp: false\n출처: {}\nsource_url: ''\nattachments: []\nepoch_clamped: {}\n발령일자_raw: {}\n---\n\n{}\n",
        yaml_string(&rule.rule_id),
        yaml_string(&rule.serial),
        yaml_string(&rule.name),
        yaml_string(&rule.rule_type),
        yaml_string(&rule.top_ministry),
        yaml_string(&rule.ministry),
        original_ministry,
        quoted_or_null(&rule.org_code),
        yaml_string(&rule.issue_no),
        issue_date,
        format_date(&rule.effective_date_raw),
        yaml_string(&rule.amendment),
        yaml_string(&rule.amendment_code),
        yaml_string(&rule.current_history),
        yaml_string(body_source),
        yaml_string(&format!(
            "https://www.law.go.kr/행정규칙/{}",
            rule.name.replace(' ', "")
        )),
        epoch_clamped,
        yaml_string(&rule.issue_date_raw),
        body
    )
}

fn quoted_or_null(value: &str) -> String {
    if value.is_empty() {
        "null".to_string()
    } else {
        yaml_string(value)
    }
}

fn yaml_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;

    #[test]
    fn parses_and_renders_admrule() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>테스트 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>행정안전부</소관부처명><발령일자>20240504</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(rule.name, "테스트 고시");
        assert!(render_markdown(&rule).contains("발령일자: 2024-05-04"));
    }

    #[test]
    fn parses_cdata_fields() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명><![CDATA[CDATA 고시]]></행정규칙명><조문내용><![CDATA[제1조 목적]]></조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(rule.name, "CDATA 고시");
        assert_eq!(rule.body, "제1조 목적");
    }

    #[test]
    fn normalizes_ministry_name_drift() {
        let date_xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>테스트 고시</행정규칙명><소관부처명>2025-10-01</소관부처명><상위부처명>기후에너지환경부</상위부처명></AdmRulService>";
        let date_rule = parse_admrule(date_xml.as_bytes(), "123").unwrap();
        assert_eq!(date_rule.top_ministry, "기후에너지환경부");
        assert_eq!(date_rule.ministry, "기후에너지환경부");
        assert_eq!(date_rule.original_ministry, "2025-10-01");

        let dot_xml = "<AdmRulService><행정규칙일련번호>124</행정규칙일련번호><행정규칙명>테스트 고시</행정규칙명><소관부처명>10.29이태원참사진상규명과재발방지를위한특별조사위원회</소관부처명></AdmRulService>";
        let dot_rule = parse_admrule(dot_xml.as_bytes(), "124").unwrap();
        assert_eq!(
            dot_rule.ministry,
            "10·29이태원참사진상규명과재발방지를위한특별조사위원회"
        );
    }

    #[test]
    fn groups_subagencies_under_canonical_parent() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>제주지방항공청 사무분장 규정</행정규칙명><행정규칙종류>훈령</행정규칙종류><소관부처명>제주지방항공청</소관부처명><상위부처명>국토교통부</상위부처명><발령일자>20240504</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(rule.top_ministry, "국토교통부");
        assert_eq!(rule.ministry, "제주지방항공청");
        assert_eq!(
            admrule_path(&rule, &mut BTreeSet::new()),
            PathBuf::from("국토교통부/제주지방항공청/훈령/제주지방항공청 사무분장 규정/본문.md")
        );
    }

    #[test]
    fn maps_safe_ministry_renames_and_keeps_original() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>문화재 테스트 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>문화재청</소관부처명><발령일자>20240504</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(rule.top_ministry, "국가유산청");
        assert_eq!(rule.ministry, "국가유산청");
        assert_eq!(rule.original_ministry, "문화재청");
        assert!(render_markdown(&rule).contains("소관부처명_원문: '문화재청'"));
    }

    #[test]
    fn quotes_yaml_sensitive_values() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>기록관 표준운영절차: 일반</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>행정안전부</소관부처명><발령일자>20240504</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert!(render_markdown(&rule).contains("행정규칙명: '기록관 표준운영절차: 일반'"));
    }

    #[test]
    fn bare_repo_uses_main_and_one_commit_per_rule() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        fs::create_dir(&cache).unwrap();
        fs::write(
            cache.join("123.xml"),
            "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙ID>ABC</행정규칙ID><행정규칙명>테스트 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>행정안전부</소관부처명><발령번호>1</발령번호><발령일자>20240504</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>",
        )
        .unwrap();
        let repo = temp.path().join("out.git");
        compile_bare_repo(&cache, &repo, None).unwrap();
        git_ok(&repo, ["fsck", "--full"]);
        assert_eq!(git_stdout(&repo, ["rev-list", "--count", "--all"]), "2");
        assert_eq!(
            git_stdout(&repo, ["symbolic-ref", "--short", "HEAD"]),
            "main"
        );
        assert!(git_stdout(&repo, ["ls-tree", "-r", "--name-only", "HEAD"]).contains("본문.md"));

        let checkout = temp.path().join("checkout");
        let status = Command::new("git")
            .args(["clone", "--quiet"])
            .arg(&repo)
            .arg(&checkout)
            .status()
            .unwrap();
        assert!(status.success());
        assert!(
            checkout
                .join("행정안전부/_본부/고시/테스트 고시/본문.md")
                .exists()
        );
    }

    #[test]
    fn bare_repo_rejects_empty_valid_entries() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        fs::create_dir(&cache).unwrap();
        let repo = temp.path().join("out.git");
        let error = compile_bare_repo(&cache, &repo, None).unwrap_err();
        assert!(error.to_string().contains("no valid admrule XML"));
        assert!(!repo.exists());
    }

    #[test]
    fn bare_repo_preserves_existing_output_when_planning_fails() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        fs::create_dir(&cache).unwrap();
        fs::write(cache.join("bad.xml"), "<AdmRulService><").unwrap();
        let repo = temp.path().join("out.git");
        fs::create_dir(&repo).unwrap();
        fs::write(repo.join("marker"), "keep").unwrap();

        let error = compile_bare_repo(&cache, &repo, None).unwrap_err();
        assert!(error.to_string().contains("invalid") || error.to_string().contains("error"));
        assert_eq!(fs::read_to_string(repo.join("marker")).unwrap(), "keep");
    }

    fn git_ok<const N: usize>(repo: &Path, args: [&str; N]) {
        let output = Command::new("git")
            .arg("-c")
            .arg("core.quotePath=false")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn git_stdout<const N: usize>(repo: &Path, args: [&str; N]) -> String {
        let output = Command::new("git")
            .arg("-c")
            .arg("core.quotePath=false")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }
}
