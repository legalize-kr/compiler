//! Compile cached law.go.kr administrative-rule XML into a bare Git repository.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use git_writer::{
    BareRepoWriter, GitTimestampKst, PreparedBlob, RepoPathBuf, hex, precompute_blob,
};
use quick_xml::Reader;
use quick_xml::events::Event;
use serde::Serialize;
use time::{Date, Month, PrimitiveDateTime, Time as CivilTime, UtcOffset};
use unicode_normalization::UnicodeNormalization;

const REPOSITORY_README: &[u8] = include_bytes!("../assets/README.md");

/// Command-line interface.
#[derive(Debug, Parser)]
#[command(name = "admrule-kr-compiler")]
#[command(
    about = "Compile cached law.go.kr administrative-rule XML into a fresh bare Git repository"
)]
struct Cli {
    /// Path to the existing `.cache/admrule/` directory.
    cache_dir: PathBuf,
    /// Output bare repository path.
    #[arg(short = 'o', long = "output", default_value = "output.git")]
    output: PathBuf,
    /// Pre-flight validation only: scan cache, emit JSON report, skip repo writes.
    #[arg(long, conflicts_with = "tree")]
    validate: bool,
    /// Emit build manifest JSON to this path. Default: no manifest.
    #[arg(long)]
    manifest: Option<PathBuf>,
    /// Limit input files for probe runs.
    #[arg(long)]
    limit: Option<usize>,
    /// Write a Markdown tree directory instead of a bare Git repository.
    #[arg(long)]
    tree: bool,
    /// Backward-compatible no-op: bare repository output is now the default.
    #[arg(long = "bare", hide = true, conflicts_with = "tree")]
    _bare: bool,
}

/// Minimal build manifest shared by non-law compilers.
#[derive(Debug, Serialize)]
struct BuildManifest {
    /// Manifest schema version.
    schema_version: u8,
    /// Final HEAD commit SHA, or an empty string in validate mode.
    head_commit_sha: String,
    /// Number of rendered entries.
    entries_total: usize,
}

/// Pre-flight validation report.
#[derive(Debug, Serialize)]
struct ValidationReport {
    /// Report schema version.
    schema_version: u8,
    /// Number of XML files under the input cache directory.
    total_xml: usize,
    /// Number of entries that can be rendered.
    entries_total: usize,
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
    /// Legal organization path used for repository grouping.
    org_path: Vec<String>,
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
    /// 별표 attachment links.
    attachments: Vec<Attachment>,
    /// Body text.
    body: String,
}

/// Parsed 별표 attachment link.
#[derive(Debug, Clone)]
struct Attachment {
    bylaw_no: String,
    branch_no: String,
    kind: String,
    title: String,
    file_link: String,
    pdf_link: String,
}

/// 2026-03-30 12:00:00 KST (UTC+9) = 2026-03-30 03:00:00 UTC.
const INITIAL_COMMIT_EPOCH: i64 = 1_774_839_600;

/// Entry point.
fn main() -> Result<()> {
    run(Cli::parse())
}

/// Executes the requested compiler mode.
fn run(cli: Cli) -> Result<()> {
    if cli.validate {
        validate_cache(&cli.cache_dir, cli.limit, cli.manifest.as_deref())
    } else if cli.tree {
        if let Some(path) = &cli.manifest {
            anyhow::bail!(
                "--manifest is only supported for bare repository builds or --validate: {}",
                path.display()
            );
        }
        compile_dir(&cli.cache_dir, &cli.output, cli.limit)
    } else {
        compile_bare_repo_with_manifest(
            &cli.cache_dir,
            &cli.output,
            cli.limit,
            cli.manifest.as_deref(),
        )
    }
}

/// Compile XML directly into a bare Git repository.
#[cfg(test)]
fn compile_bare_repo(cache_dir: &Path, output: &Path, limit: Option<usize>) -> Result<()> {
    compile_bare_repo_with_manifest(cache_dir, output, limit, None)
}

/// Compile XML directly into a bare Git repository, optionally writing a manifest.
fn compile_bare_repo_with_manifest(
    cache_dir: &Path,
    output: &Path,
    limit: Option<usize>,
    manifest: Option<&Path>,
) -> Result<()> {
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
        if let Some(previous_path) = &entry.previous_path {
            repo.commit_bot_file_with_deletions(
                &RepoPathBuf::file(&entry.path),
                PreparedBlob::from_parts(&entry.content, blob_sha, &compressed_blob),
                &[RepoPathBuf::file(previous_path)],
                &entry.message,
                GitTimestampKst::from_epoch(entry.timestamp),
            )?;
        } else {
            repo.commit_bot_file(
                &RepoPathBuf::file(&entry.path),
                &entry.content,
                blob_sha,
                &compressed_blob,
                &entry.message,
                GitTimestampKst::from_epoch(entry.timestamp),
            )?;
        }
    }
    let head_sha = repo.finish()?;
    if let Some(path) = manifest {
        write_manifest(
            path,
            &BuildManifest {
                schema_version: 1,
                head_commit_sha: hex(&head_sha),
                entries_total: entries.len(),
            },
        )?;
    }
    eprintln!("committed {} admrule markdown files", entries.len());
    Ok(())
}

/// Scans input cache and emits a JSON validation report without writing output.
fn validate_cache(cache_dir: &Path, limit: Option<usize>, manifest: Option<&Path>) -> Result<()> {
    let total_xml = read_xml_files(cache_dir)?.len();
    let entries = render_admrule_entries(cache_dir, limit)?;
    let report = ValidationReport {
        schema_version: 1,
        total_xml,
        entries_total: entries.len(),
    };
    let json = serde_json::to_string_pretty(&report)
        .context("failed to serialize validation report as JSON")?;
    println!("{json}");
    if let Some(path) = manifest {
        write_manifest(
            path,
            &BuildManifest {
                schema_version: 1,
                head_commit_sha: String::new(),
                entries_total: entries.len(),
            },
        )?;
    }
    Ok(())
}

/// Writes a pretty-printed manifest JSON payload.
fn write_manifest(path: &Path, manifest: &BuildManifest) -> Result<()> {
    let json = serde_json::to_string_pretty(manifest)
        .context("failed to serialize build manifest as JSON")?;
    fs::write(path, json).with_context(|| format!("failed to write manifest to {}", path.display()))
}

#[derive(Debug)]
struct ImportEntry {
    path: String,
    previous_path: Option<String>,
    identity: String,
    content: Vec<u8>,
    message: String,
    timestamp: i64,
    sort_date: String,
    sort_id: u64,
}

type PathRegistry = BTreeMap<String, String>;

fn render_admrule_entries(cache_dir: &Path, limit: Option<usize>) -> Result<Vec<ImportEntry>> {
    let mut files = read_xml_files(cache_dir)?;
    if let Some(limit) = limit {
        files.truncate(limit);
    }
    let mut registry = PathRegistry::new();
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
            previous_path: None,
            identity: rule.serial.clone(),
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
    assign_previous_paths(&mut entries);
    Ok(entries)
}

fn assign_previous_paths(entries: &mut [ImportEntry]) {
    let mut latest_paths = BTreeMap::new();
    for entry in entries {
        if let Some(previous_path) = latest_paths.insert(entry.identity.clone(), entry.path.clone())
            && previous_path != entry.path
        {
            entry.previous_path = Some(previous_path);
        }
    }
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
    if is_valid_compact_date(&digits) {
        if digits.as_str() < "19700101" {
            "19700101".to_string()
        } else {
            digits
        }
    } else {
        "19700101".to_string()
    }
}

fn is_valid_compact_date(date: &str) -> bool {
    if date.len() != 8 || !date.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    let Ok(year) = date[0..4].parse::<i32>() else {
        return false;
    };
    let Ok(month_num) = date[4..6].parse::<u8>() else {
        return false;
    };
    let Ok(month) = Month::try_from(month_num) else {
        return false;
    };
    let Ok(day) = date[6..8].parse::<u8>() else {
        return false;
    };
    Date::from_calendar_date(year, month, day).is_ok()
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
    let entries = render_admrule_entries(cache_dir, limit)?;
    for entry in &entries {
        if let Some(previous_path) = &entry.previous_path {
            let previous = output.join(previous_path);
            if previous.exists() {
                fs::remove_file(&previous)
                    .with_context(|| format!("failed to remove {}", previous.display()))?;
            }
        }
        let target = output.join(&entry.path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&target, &entry.content)?;
    }
    eprintln!("written {} admrule markdown files", entries.len());
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
    let attachments = collect_attachments(raw)?;
    let serial = first(&fields, &["행정규칙일련번호", "ID"])
        .unwrap_or(fallback_serial)
        .to_string();
    let body = collect_body(&fields, &["조문내용", "본문", "내용"]);
    let raw_ministry = nfc(first(&fields, &["소관부처명"]).unwrap_or(""));
    let raw_parent = nfc(first(&fields, &["상위부처명"]).unwrap_or(""));
    let raw_department_org = nfc(first(&fields, &["담당부서기관명"]).unwrap_or(""));
    let (raw_top_ministry, resolved_ministry) =
        resolve_ministry_names(&raw_ministry, &raw_parent, &raw_department_org);
    let org_path = resolve_org_path(&raw_top_ministry, &resolved_ministry);
    let top_ministry = org_path
        .first()
        .cloned()
        .unwrap_or_else(|| raw_top_ministry.clone());
    let ministry = org_path
        .last()
        .cloned()
        .unwrap_or_else(|| resolved_ministry.clone());
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
        org_path,
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
        attachments,
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

/// Extract structured 별표 download links without writing binary files.
fn collect_attachments(raw: &[u8]) -> Result<Vec<Attachment>> {
    let mut reader = Reader::from_reader(raw);
    reader.config_mut().trim_text(true);
    let mut fields: BTreeMap<String, String> = BTreeMap::new();
    let mut attachments = Vec::new();
    let mut current = String::new();
    let mut in_attachment = false;
    let mut attachment_root = String::new();
    let mut depth = 0usize;

    loop {
        match reader.read_event()? {
            Event::Start(event) => {
                let tag = String::from_utf8_lossy(event.name().as_ref()).to_string();
                if !in_attachment && matches!(tag.as_str(), "별표" | "별표단위") {
                    in_attachment = true;
                    attachment_root = tag;
                    depth = 1;
                    fields.clear();
                    current.clear();
                } else if in_attachment {
                    depth += 1;
                    current = tag;
                }
            }
            Event::Text(text) if in_attachment && !current.is_empty() => {
                let value = nfc(text.decode()?.trim());
                if !value.is_empty() {
                    fields.entry(current.clone()).or_insert(value);
                }
            }
            Event::CData(text) if in_attachment && !current.is_empty() => {
                let value = nfc(text.decode()?.trim());
                if !value.is_empty() {
                    fields.entry(current.clone()).or_insert(value);
                }
            }
            Event::End(event) if in_attachment => {
                let tag = String::from_utf8_lossy(event.name().as_ref()).to_string();
                if depth == 1 && tag == attachment_root {
                    if let Some(attachment) = attachment_from_fields(&fields, attachments.len()) {
                        attachments.push(attachment);
                    }
                    in_attachment = false;
                    attachment_root.clear();
                    current.clear();
                    depth = 0;
                } else {
                    depth = depth.saturating_sub(1);
                    current.clear();
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }

    Ok(attachments)
}

fn attachment_from_fields(fields: &BTreeMap<String, String>, index: usize) -> Option<Attachment> {
    let file_link = absolute_law_url(first_attachment(
        fields,
        &["별표서식파일링크", "별표파일링크"],
    ));
    let pdf_link = absolute_law_url(first_attachment(
        fields,
        &["별표서식PDF파일링크", "별표PDF파일링크"],
    ));
    if file_link.is_empty() && pdf_link.is_empty() {
        return None;
    }
    Some(Attachment {
        bylaw_no: first_attachment(fields, &["별표번호"])
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("별표 {}", index + 1)),
        branch_no: first_attachment(fields, &["별표가지번호"])
            .unwrap_or("")
            .to_string(),
        kind: first_attachment(fields, &["별표구분"])
            .filter(|value| !value.is_empty())
            .unwrap_or("별표")
            .to_string(),
        title: first_attachment(fields, &["별표제목", "별표명"])
            .unwrap_or("")
            .to_string(),
        file_link,
        pdf_link,
    })
}

fn first_attachment<'a>(fields: &'a BTreeMap<String, String>, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| fields.get(*key).map(String::as_str))
}

fn absolute_law_url(value: Option<&str>) -> String {
    let value = value.unwrap_or("").trim();
    if value.is_empty() {
        String::new()
    } else if value.starts_with('/') {
        format!("https://www.law.go.kr{value}")
    } else if value.starts_with("http://") || value.starts_with("https://") {
        value.to_string()
    } else {
        format!("https://www.law.go.kr/{value}")
    }
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
        "국립환경인력개발원" => Some("국립환경인재개발원"),
        "산업통상자원부" => Some("산업통상부"),
        "기획재정부" => Some("재정경제부"),
        "행정자치부" => Some("행정안전부"),
        "미래창조과학부" => Some("과학기술정보통신부"),
        "중소기업청" => Some("중소벤처기업부"),
        "국가보훈처" => Some("국가보훈부"),
        "방송통신위원회" => Some("방송미디어통신위원회"),
        "방송통신사무소" => Some("방송미디어통신사무소"),
        "여성가족부" => Some("성평등가족부"),
        "식품의약품안전청" => Some("식품의약품안전처"),
        "평생교육진흥원" => Some("국가평생교육진흥원"),
        "중앙민방위방재교육원" | "국가민방위재난안전교육원" => {
            Some("국가재난안전교육원")
        }
        _ => None,
    }
}

fn resolve_ministry_names(ministry: &str, parent: &str, department_org: &str) -> (String, String) {
    let mut agency = normalize_ministry_name(ministry, parent);
    let normalized_parent = normalize_ministry_name(parent, "");
    let (department_agency, department_unit) = split_department_org_name(department_org);
    let mut top = if parent.trim().is_empty() {
        agency.clone()
    } else {
        normalized_parent.clone()
    };

    if should_use_current_department_root_for_stale_ministry(&top, &agency, &department_agency)
        || should_collapse_historical_root_ministry(&top, &agency)
    {
        agency.clone_from(&top);
    } else if let Some((chain_top, chain_agency)) =
        split_parent_agency_chain(&normalized_parent, &department_agency)
    {
        top = chain_top;
        if agency.is_empty() || agency == department_unit || agency == normalized_parent {
            agency = chain_agency;
        }
    } else if agency == department_agency && is_root_level_agency(&agency) {
        top.clone_from(&agency);
    } else if agency == department_unit && !department_agency.is_empty() {
        agency = department_agency.clone();
    } else if agency == top && should_use_department_root(&top, &department_agency) {
        top = department_agency.clone();
        agency = department_agency;
    }

    if top.is_empty() {
        top.clone_from(&agency);
    }
    if agency.is_empty() {
        agency.clone_from(&top);
    }
    (top, agency)
}

fn should_use_current_department_root_for_stale_ministry(
    top: &str,
    agency: &str,
    department_agency: &str,
) -> bool {
    matches!(
        (top, agency, department_agency),
        (
            "과학기술정보통신부",
            "방송미디어통신위원회",
            "과학기술정보통신부"
        ) | ("기후에너지환경부", "국토교통부", "기후에너지환경부")
    )
}

fn should_collapse_historical_root_ministry(top: &str, agency: &str) -> bool {
    match agency {
        "문교부" | "교육인적자원부" => top == "교육부",
        "교육과학기술부" => matches!(top, "교육부" | "과학기술정보통신부"),
        "노동부" => top == "고용노동부",
        "외교통상부" => top == "외교부",
        "국토해양부" => matches!(top, "국토교통부" | "해양수산부" | "기후에너지환경부"),
        "지식경제부" => top == "산업통상부",
        "정보통신부" => top == "과학기술정보통신부",
        "문화관광부" => top == "문화체육관광부",
        "안전행정부" => top == "행정안전부",
        "보건복지가족부" => top == "보건복지부",
        "농림부" => top == "농림축산식품부",
        "농림수산부" | "농림수산식품부" => {
            matches!(top, "농림축산식품부" | "해양수산부")
        }
        _ => false,
    }
}

fn should_use_department_root(top: &str, department_agency: &str) -> bool {
    if department_agency.is_empty() || !is_root_level_agency(department_agency) {
        return false;
    }
    !is_root_level_agency(top)
        || matches!((top, department_agency), ("산업통상부", "기후에너지환경부"))
}

fn resolve_org_path(top: &str, agency: &str) -> Vec<String> {
    let top = normalize_ministry_name(top, "");
    let agency = normalize_ministry_name(agency, "");
    if agency.is_empty() {
        return build_legal_org_path(&top);
    }

    if let Some(parent) = legal_parent_agency(&agency)
        && top != agency
        && top != parent
        && is_root_level_agency(&agency)
    {
        return build_legal_org_path(&agency);
    }

    let mut path = build_legal_org_path(&top);
    if agency != top && !agency.is_empty() && !path.iter().any(|part| part == &agency) {
        path.push(agency);
    }
    path
}

fn build_legal_org_path(agency: &str) -> Vec<String> {
    let agency = normalize_ministry_name(agency, "");
    if agency.is_empty() {
        return Vec::new();
    }
    if let Some(parent) = legal_parent_agency(&agency) {
        let mut path = build_legal_org_path(parent);
        if !path.iter().any(|part| part == &agency) {
            path.push(agency);
        }
        path
    } else {
        vec![agency]
    }
}

fn legal_parent_agency(value: &str) -> Option<&'static str> {
    match value {
        "국가정보원" => Some("대통령"),
        "국무조정실"
        | "국무총리비서실"
        | "기획예산처"
        | "인사혁신처"
        | "법제처"
        | "식품의약품안전처"
        | "국가데이터처"
        | "지식재산처"
        | "공정거래위원회"
        | "국민권익위원회"
        | "금융위원회"
        | "개인정보보호위원회"
        | "원자력안전위원회" => Some("국무총리"),
        "국세청" | "관세청" | "조달청" => Some("재정경제부"),
        "재외동포청" => Some("외교부"),
        "병무청" | "방위사업청" => Some("국방부"),
        "경찰청" | "소방청" => Some("행정안전부"),
        "국가유산청" => Some("문화체육관광부"),
        "농촌진흥청" | "산림청" => Some("농림축산식품부"),
        "질병관리청" => Some("보건복지부"),
        "기상청" => Some("기후에너지환경부"),
        "해양경찰청" => Some("해양수산부"),
        "방송미디어통신위원회" | "국가교육위원회" => Some("대통령"),
        "방송미디어통신사무소" => Some("방송미디어통신위원회"),
        "국립전파연구원" | "중앙전파관리소" => Some("과학기술정보통신부"),
        "전파시험인증센터" => Some("국립전파연구원"),
        "위성전파감시센터" | "전파관리소" => Some("중앙전파관리소"),
        "우주항공청" => Some("과학기술정보통신부"),
        "행정중심복합도시건설청" | "새만금개발청" => Some("국토교통부"),
        "대검찰청" => Some("법무부"),
        "국립농산물품질관리원" => Some("농림축산식품부"),
        "민주평화통일자문회의사무처" => Some("대통령"),
        "수도권매립지관리공사" => Some("기후에너지환경부"),
        "국가평생교육진흥원" => Some("교육부"),
        "국가재난안전교육원" | "국립재난안전연구원" => Some("행정안전부"),
        _ => None,
    }
}

fn split_department_org_name(value: &str) -> (String, String) {
    let text = normalize_ministry_name(value, "");
    let Some((outer, inner_with_suffix)) = text.split_once('(') else {
        return (text, String::new());
    };
    let inner = inner_with_suffix
        .strip_suffix(')')
        .unwrap_or(inner_with_suffix)
        .trim();
    (
        normalize_ministry_name(outer, ""),
        normalize_ministry_name(inner, ""),
    )
}

fn split_parent_agency_chain(parent: &str, agency: &str) -> Option<(String, String)> {
    if agency.is_empty() || parent == agency {
        return None;
    }
    let prefix = parent.strip_suffix(agency)?.trim();
    if prefix.is_empty() {
        return None;
    }
    Some((normalize_ministry_name(prefix, ""), agency.to_string()))
}

fn is_root_level_agency(value: &str) -> bool {
    legal_parent_agency(value).is_some()
        || matches!(
            value,
            "대통령"
                | "국무총리"
                | "교육부"
                | "외교부"
                | "통일부"
                | "법무부"
                | "국방부"
                | "행정안전부"
                | "문화체육관광부"
                | "농림축산식품부"
                | "산업통상부"
                | "보건복지부"
                | "기후에너지환경부"
                | "고용노동부"
                | "성평등가족부"
                | "국토교통부"
                | "해양수산부"
                | "중소벤처기업부"
                | "재정경제부"
                | "과학기술정보통신부"
                | "국가보훈부"
                | "국가인권위원회"
                | "중앙선거관리위원회"
                | "고위공직자범죄수사처"
                | "진실화해를위한과거사정리위원회"
                | "세월호 선체조사위원회"
                | "친일반민족행위자재산조사위원회"
                | "10·29이태원참사진상규명과재발방지를위한특별조사위원회"
        )
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
fn admrule_path(rule: &Admrule, registry: &mut PathRegistry) -> PathBuf {
    let org_parts = admrule_org_path_parts(rule);
    let rule_type = safe_path_part(&rule.rule_type);
    let name = safe_path_part(&rule.name);
    let prefix = org_parts.join("/");
    let base = format!("{prefix}/{rule_type}/{name}/본문.md");
    if claim_path(registry, &base, &rule.serial) {
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
        let suffixed = format!("{prefix}/{rule_type}/{name}_{suffix}/본문.md");
        if claim_path(registry, &suffixed, &rule.serial) {
            return PathBuf::from(suffixed);
        }
    }
    let mut idx = 2usize;
    loop {
        let suffixed = format!("{prefix}/{rule_type}/{name}_{}_{idx}/본문.md", rule.serial);
        if claim_path(registry, &suffixed, &rule.serial) {
            return PathBuf::from(suffixed);
        }
        idx += 1;
    }
}

fn claim_path(registry: &mut PathRegistry, path: &str, identity: &str) -> bool {
    match registry.get(path) {
        None => {
            registry.insert(path.to_string(), identity.to_string());
            true
        }
        Some(existing) if existing == identity => true,
        Some(_) => false,
    }
}

fn admrule_org_path_parts(rule: &Admrule) -> Vec<String> {
    let mut parts: Vec<String> = if rule.org_path.is_empty() {
        vec![rule.top_ministry.clone()]
    } else {
        rule.org_path.clone()
    };
    if parts.is_empty() {
        parts.push("_".to_string());
    }
    if parts.len() == 1 && rule.ministry == rule.top_ministry {
        parts.push("_본부".to_string());
    }
    parts
        .into_iter()
        .map(|part| safe_path_part(&part))
        .collect()
}

/// Convert compact dates to ISO dates.
fn format_date(raw: &str) -> String {
    let digits = raw.replace(['.', '-'], "");
    if is_valid_compact_date(&digits) {
        format!("{}-{}-{}", &digits[..4], &digits[4..6], &digits[6..8])
    } else {
        raw.to_string()
    }
}

/// Clamp pre-epoch dates the same way as the Python pipeline.
fn issue_date(raw: &str) -> (String, bool) {
    let digits = raw.replace(['.', '-'], "");
    if digits.len() == 8
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && (!is_valid_compact_date(&digits) || digits.as_str() < "19700101")
    {
        ("1970-01-01".to_string(), true)
    } else {
        (format_date(raw), false)
    }
}

fn render_attachments_yaml(attachments: &[Attachment]) -> String {
    if attachments.is_empty() {
        return "첨부파일: []\n".to_string();
    }
    let mut out = String::from("첨부파일:\n");
    for attachment in attachments {
        out.push_str(&format!(
            "- 별표번호: {}\n  별표가지번호: {}\n  별표구분: {}\n  제목: {}\n",
            yaml_string(&attachment.bylaw_no),
            yaml_string(&attachment.branch_no),
            yaml_string(&attachment.kind),
            yaml_string(&attachment.title),
        ));
        if !attachment.file_link.is_empty() {
            out.push_str(&format!(
                "  파일링크: {}\n",
                yaml_string(&attachment.file_link)
            ));
        }
        if !attachment.pdf_link.is_empty() {
            out.push_str(&format!(
                "  PDF링크: {}\n",
                yaml_string(&attachment.pdf_link)
            ));
        }
    }
    out
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
    let org_path = if rule.org_path.is_empty() {
        String::new()
    } else {
        let items = rule
            .org_path
            .iter()
            .map(|part| format!("- {}\n", yaml_string(part)))
            .collect::<String>();
        format!("기관경로:\n{items}")
    };
    let attachments_yaml = render_attachments_yaml(&rule.attachments);
    format!(
        "---\n행정규칙ID: {}\n행정규칙일련번호: {}\n행정규칙명: {}\n행정규칙종류: {}\n상위기관명: {}\n소관부처명: {}\n{}{}기관코드: {}\n발령번호: {}\n발령일자: {}\n시행일자: {}\n제개정구분: {}\n제개정구분코드: {}\n현행연혁구분: {}\n본문출처: {}\n출처: {}\n{}발령일자보정: {}\n발령일자원문: {}\n---\n\n{}\n",
        yaml_string(&rule.rule_id),
        yaml_string(&rule.serial),
        yaml_string(&rule.name),
        yaml_string(&rule.rule_type),
        yaml_string(&rule.top_ministry),
        yaml_string(&rule.ministry),
        original_ministry,
        org_path,
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
        attachments_yaml,
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
        assert!(render_markdown(&rule).contains("첨부파일: []"));
    }

    #[test]
    fn path_registry_reuses_path_for_same_rule_serial_revisions() {
        let first = parse_admrule(
            "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>같은 이름</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>행정안전부</소관부처명><발령번호>제1호</발령번호></AdmRulService>".as_bytes(),
            "123",
        )
        .unwrap();
        let second = parse_admrule(
            "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>같은 이름</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>행정안전부</소관부처명><발령번호>제2호</발령번호></AdmRulService>".as_bytes(),
            "123",
        )
        .unwrap();
        let other = parse_admrule(
            "<AdmRulService><행정규칙일련번호>456</행정규칙일련번호><행정규칙명>같은 이름</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>행정안전부</소관부처명><발령번호>제3호</발령번호></AdmRulService>".as_bytes(),
            "456",
        )
        .unwrap();
        let mut registry = PathRegistry::new();
        let base = PathBuf::from("행정안전부/_본부/고시/같은 이름/본문.md");
        assert_eq!(admrule_path(&first, &mut registry), base);
        assert_eq!(admrule_path(&second, &mut registry), base);
        assert_eq!(
            admrule_path(&other, &mut registry),
            PathBuf::from("행정안전부/_본부/고시/같은 이름_제3호/본문.md")
        );
    }

    #[test]
    fn parses_cdata_fields() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명><![CDATA[CDATA 고시]]></행정규칙명><조문내용><![CDATA[제1조 목적]]></조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(rule.name, "CDATA 고시");
        assert_eq!(rule.body, "제1조 목적");
    }

    #[test]
    fn parses_and_renders_attachment_links() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>첨부 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>행정안전부</소관부처명><발령일자>20240504</발령일자><조문내용>제1조 목적</조문내용><별표><별표번호>0001</별표번호><별표가지번호>00</별표가지번호><별표구분>별표</별표구분><별표제목><![CDATA[수수료]]></별표제목><별표서식파일링크>/LSW/flDownload.do?flSeq=1</별표서식파일링크><별표서식PDF파일링크>/LSW/flDownload.do?flSeq=2</별표서식PDF파일링크></별표></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();

        assert_eq!(rule.attachments.len(), 1);
        assert_eq!(
            rule.attachments[0].file_link,
            "https://www.law.go.kr/LSW/flDownload.do?flSeq=1"
        );
        let markdown = render_markdown(&rule);
        assert!(markdown.contains("첨부파일:\n- 별표번호: '0001'"));
        assert!(markdown.contains("파일링크: 'https://www.law.go.kr/LSW/flDownload.do?flSeq=1'"));
        assert!(markdown.contains("PDF링크: 'https://www.law.go.kr/LSW/flDownload.do?flSeq=2'"));
    }

    #[test]
    fn invalid_compact_dates_fall_back_to_epoch_for_commit_timestamp() {
        assert_eq!(compact_date_or_epoch("20240229"), "20240229");
        assert_eq!(compact_date_or_epoch("20240231"), "19700101");
        assert_eq!(compact_date_or_epoch("20241301"), "19700101");
        assert_eq!(format_date("20240231"), "20240231");
        assert_eq!(issue_date("20240231"), ("1970-01-01".to_string(), true));
        assert_eq!(compact_date_or_epoch("19691231"), "19700101");
        assert_eq!(
            commit_timestamp("20240231").unwrap(),
            commit_timestamp("19700101").unwrap()
        );
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

        assert_eq!(
            normalize_ministry_name("국립환경인력개발원", ""),
            "국립환경인재개발원"
        );
        assert_eq!(normalize_ministry_name("행정자치부", ""), "행정안전부");
        assert_eq!(normalize_ministry_name("기획재정부", ""), "재정경제부");
        assert_eq!(
            normalize_ministry_name("중앙민방위방재교육원", ""),
            "국가재난안전교육원"
        );
        assert_eq!(
            normalize_ministry_name("국가민방위재난안전교육원", ""),
            "국가재난안전교육원"
        );
    }

    #[test]
    fn groups_subagencies_under_canonical_parent() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>제주지방항공청 사무분장 규정</행정규칙명><행정규칙종류>훈령</행정규칙종류><소관부처명>제주지방항공청</소관부처명><상위부처명>국토교통부</상위부처명><발령일자>20240504</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(rule.top_ministry, "국토교통부");
        assert_eq!(rule.ministry, "제주지방항공청");
        assert_eq!(
            admrule_path(&rule, &mut PathRegistry::new()),
            PathBuf::from("국토교통부/제주지방항공청/훈령/제주지방항공청 사무분장 규정/본문.md")
        );
    }

    #[test]
    fn maps_safe_ministry_renames_and_keeps_original() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>문화재 테스트 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>문화재청</소관부처명><발령일자>20240504</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(rule.top_ministry, "문화체육관광부");
        assert_eq!(rule.ministry, "국가유산청");
        assert_eq!(rule.org_path, ["문화체육관광부", "국가유산청"]);
        assert_eq!(rule.original_ministry, "문화재청");
        assert!(render_markdown(&rule).contains("소관부처명_원문: '문화재청'"));
    }

    #[test]
    fn splits_compound_parent_with_department_agency() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>위해성평가 실시 등의 대상이 되는 환경유해인자의 목록</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>환경보건정책과</소관부처명><상위부처명>기후에너지환경부 국립환경과학원</상위부처명><담당부서기관명>국립환경과학원(환경보건정책과)</담당부서기관명><발령일자>20251103</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(rule.top_ministry, "기후에너지환경부");
        assert_eq!(rule.ministry, "국립환경과학원");
        assert_eq!(rule.original_ministry, "환경보건정책과");
        assert_eq!(
            admrule_path(&rule, &mut PathRegistry::new()),
            PathBuf::from(
                "기후에너지환경부/국립환경과학원/고시/위해성평가 실시 등의 대상이 되는 환경유해인자의 목록/본문.md"
            )
        );
    }

    #[test]
    fn uses_current_top_level_agency_for_split_legacy_ministry() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>무인도서 관리유형 재지정(변경) 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>국토해양부</소관부처명><상위부처명>국토해양부</상위부처명><담당부서기관명>해양수산부(해양영토과)</담당부서기관명><발령일자>20111104</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(rule.top_ministry, "해양수산부");
        assert_eq!(rule.ministry, "해양수산부");
        assert_eq!(rule.original_ministry, "국토해양부");
        assert_eq!(
            admrule_path(&rule, &mut PathRegistry::new()),
            PathBuf::from("해양수산부/_본부/고시/무인도서 관리유형 재지정(변경) 고시/본문.md")
        );
    }

    #[test]
    fn collapses_historical_root_ministry_under_current_top() {
        let cases = [
            ("교육부", "문교부"),
            ("교육부", "교육인적자원부"),
            ("교육부", "교육과학기술부"),
            ("과학기술정보통신부", "교육과학기술부"),
            ("고용노동부", "노동부"),
            ("외교부", "외교통상부"),
            ("해양수산부", "국토해양부"),
            ("기후에너지환경부", "국토해양부"),
            ("산업통상부", "지식경제부"),
            ("과학기술정보통신부", "정보통신부"),
            ("문화체육관광부", "문화관광부"),
            ("행정안전부", "안전행정부"),
            ("보건복지부", "보건복지가족부"),
            ("농림축산식품부", "농림부"),
            ("농림축산식품부", "농림수산부"),
            ("농림축산식품부", "농림수산식품부"),
            ("해양수산부", "농림수산식품부"),
        ];
        for (current, historical) in cases {
            let xml = format!(
                "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>{historical} 테스트 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>{historical}</소관부처명><상위부처명>{current}</상위부처명><담당부서기관명>{current}(운영지원과)</담당부서기관명><발령일자>20240504</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>"
            );
            let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
            assert_eq!(rule.top_ministry, current);
            assert_eq!(rule.ministry, current);
            assert_eq!(rule.org_path, [current]);
            assert_eq!(rule.original_ministry, historical);
            assert_eq!(
                admrule_path(&rule, &mut PathRegistry::new()),
                PathBuf::from(format!(
                    "{current}/_본부/고시/{historical} 테스트 고시/본문.md"
                ))
            );
        }
    }

    #[test]
    fn applies_legal_parent_chain_for_external_agencies() {
        let own_xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>병무청 예규</행정규칙명><행정규칙종류>예규</행정규칙종류><소관부처명>병무청</소관부처명><상위부처명>병무청</상위부처명><발령일자>20260101</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let own_rule = parse_admrule(own_xml.as_bytes(), "123").unwrap();
        assert_eq!(own_rule.top_ministry, "국방부");
        assert_eq!(own_rule.ministry, "병무청");
        assert_eq!(own_rule.org_path, ["국방부", "병무청"]);
        assert_eq!(
            admrule_path(&own_rule, &mut PathRegistry::new()),
            PathBuf::from("국방부/병무청/예규/병무청 예규/본문.md")
        );

        let sub_xml = "<AdmRulService><행정규칙일련번호>124</행정규칙일련번호><행정규칙명>국립산림과학원 훈령</행정규칙명><행정규칙종류>훈령</행정규칙종류><소관부처명>국립산림과학원</소관부처명><상위부처명>산림청</상위부처명><발령일자>20260101</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let sub_rule = parse_admrule(sub_xml.as_bytes(), "124").unwrap();
        assert_eq!(
            sub_rule.org_path,
            ["농림축산식품부", "산림청", "국립산림과학원"]
        );
        assert_eq!(
            admrule_path(&sub_rule, &mut PathRegistry::new()),
            PathBuf::from("농림축산식품부/산림청/국립산림과학원/훈령/국립산림과학원 훈령/본문.md")
        );
    }

    #[test]
    fn applies_legal_parent_chain_for_remaining_verified_roots() {
        assert_eq!(
            resolve_org_path("대검찰청", "대검찰청"),
            ["법무부", "대검찰청"]
        );
        assert_eq!(
            resolve_org_path("국립농산물품질관리원", "국립농산물품질관리원"),
            ["농림축산식품부", "국립농산물품질관리원"]
        );
        assert_eq!(
            resolve_org_path("민주평화통일자문회의사무처", "민주평화통일자문회의사무처"),
            ["대통령", "민주평화통일자문회의사무처"]
        );
        assert_eq!(
            resolve_org_path("국립전파연구원", "국립전파연구원"),
            ["과학기술정보통신부", "국립전파연구원"]
        );
        assert_eq!(
            resolve_org_path("국민안전처", "국립재난안전연구원"),
            ["행정안전부", "국립재난안전연구원"]
        );
        assert_eq!(
            resolve_org_path("국민안전처", "국가재난안전교육원"),
            ["행정안전부", "국가재난안전교육원"]
        );
        assert_eq!(
            resolve_org_path("중앙전파관리소", "중앙전파관리소"),
            ["과학기술정보통신부", "중앙전파관리소"]
        );
        assert_eq!(
            resolve_org_path("전파관리소", "전파관리소"),
            ["과학기술정보통신부", "중앙전파관리소", "전파관리소"]
        );
    }

    #[test]
    fn applies_legal_parent_for_prime_minister_and_presidential_agencies() {
        let law_xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>법제처 훈령</행정규칙명><행정규칙종류>훈령</행정규칙종류><소관부처명>법제처</소관부처명><상위부처명>법무부</상위부처명><담당부서기관명>법제처(운영지원과)</담당부서기관명><발령일자>20260101</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let law_rule = parse_admrule(law_xml.as_bytes(), "123").unwrap();
        assert_eq!(law_rule.org_path, ["국무총리", "법제처"]);
        assert_eq!(
            admrule_path(&law_rule, &mut PathRegistry::new()),
            PathBuf::from("국무총리/법제처/훈령/법제처 훈령/본문.md")
        );

        let education_xml = "<AdmRulService><행정규칙일련번호>124</행정규칙일련번호><행정규칙명>국가교육위원회 규칙</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>국가교육위원회</소관부처명><상위부처명>교육부</상위부처명><담당부서기관명>국가교육위원회(운영지원과)</담당부서기관명><발령일자>20260101</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let education_rule = parse_admrule(education_xml.as_bytes(), "124").unwrap();
        assert_eq!(education_rule.org_path, ["대통령", "국가교육위원회"]);
        assert_eq!(
            admrule_path(&education_rule, &mut PathRegistry::new()),
            PathBuf::from("대통령/국가교육위원회/고시/국가교육위원회 규칙/본문.md")
        );

        let office_xml = "<AdmRulService><행정규칙일련번호>125</행정규칙일련번호><행정규칙명>방송미디어통신사무소 세칙</행정규칙명><행정규칙종류>훈령</행정규칙종류><소관부처명>방송통신사무소</소관부처명><상위부처명>방송통신위원회</상위부처명><담당부서기관명>방송미디어통신사무소</담당부서기관명><발령일자>20260202</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let office_rule = parse_admrule(office_xml.as_bytes(), "125").unwrap();
        assert_eq!(
            office_rule.org_path,
            ["대통령", "방송미디어통신위원회", "방송미디어통신사무소"]
        );
        assert_eq!(
            admrule_path(&office_rule, &mut PathRegistry::new()),
            PathBuf::from(
                "대통령/방송미디어통신위원회/방송미디어통신사무소/훈령/방송미디어통신사무소 세칙/본문.md"
            )
        );
    }

    #[test]
    fn does_not_replace_current_root_with_unrelated_department_agency() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>국방전자기스펙트럼 업무 훈령</행정규칙명><행정규칙종류>훈령</행정규칙종류><소관부처명>국방부</소관부처명><상위부처명>국방부</상위부처명><담당부서기관명>법제처(법제지원총괄과)</담당부서기관명><발령일자>20260101</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(rule.org_path, ["국방부"]);
        assert_eq!(
            admrule_path(&rule, &mut PathRegistry::new()),
            PathBuf::from("국방부/_본부/훈령/국방전자기스펙트럼 업무 훈령/본문.md")
        );
    }

    #[test]
    fn uses_verified_department_root_for_current_split_functions() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>전력산업 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>산업통상부</소관부처명><상위부처명>산업통상부</상위부처명><담당부서기관명>기후에너지환경부(전력산업정책과)</담당부서기관명><발령일자>20260101</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(rule.org_path, ["기후에너지환경부"]);
        assert_eq!(
            admrule_path(&rule, &mut PathRegistry::new()),
            PathBuf::from("기후에너지환경부/_본부/고시/전력산업 고시/본문.md")
        );
    }

    #[test]
    fn keeps_stale_broadcast_commission_rule_under_current_science_ministry() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>이동통신 주파수 할당</행정규칙명><행정규칙종류>공고</행정규칙종류><소관부처명>방송통신위원회</소관부처명><상위부처명>과학기술정보통신부</상위부처명><담당부서기관명>과학기술정보통신부(주파수정책과)</담당부서기관명><발령일자>20110629</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(rule.org_path, ["과학기술정보통신부"]);
        assert_eq!(
            admrule_path(&rule, &mut PathRegistry::new()),
            PathBuf::from("과학기술정보통신부/_본부/공고/이동통신 주파수 할당/본문.md")
        );
    }

    #[test]
    fn uses_current_environment_ministry_for_river_rules() {
        let xml = "<AdmRulService><행정규칙일련번호>2100000079411</행정규칙일련번호><행정규칙명>하천에 관한 사무처리규정</행정규칙명><행정규칙종류>훈령</행정규칙종류><소관부처명>국토교통부</소관부처명><상위부처명>기후에너지환경부</상위부처명><담당부서기관명>기후에너지환경부(하천계획과)</담당부서기관명><발령일자>20170307</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "2100000079411").unwrap();
        assert_eq!(rule.org_path, ["기후에너지환경부"]);
        assert_eq!(
            admrule_path(&rule, &mut PathRegistry::new()),
            PathBuf::from("기후에너지환경부/_본부/훈령/하천에 관한 사무처리규정/본문.md")
        );
    }

    #[test]
    fn maps_abolished_safety_ministry_subagencies() {
        let education_xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>중앙민방위방재교육원 위임·전결규정</행정규칙명><행정규칙종류>훈령</행정규칙종류><소관부처명>중앙민방위방재교육원</소관부처명><상위부처명>국민안전처</상위부처명><담당부서기관명>중앙민방위방재교육원</담당부서기관명><발령일자>20140414</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let education_rule = parse_admrule(education_xml.as_bytes(), "123").unwrap();
        assert_eq!(education_rule.top_ministry, "행정안전부");
        assert_eq!(education_rule.ministry, "국가재난안전교육원");
        assert_eq!(
            education_rule.org_path,
            ["행정안전부", "국가재난안전교육원"]
        );
        assert_eq!(education_rule.original_ministry, "중앙민방위방재교육원");
        assert_eq!(
            admrule_path(&education_rule, &mut PathRegistry::new()),
            PathBuf::from(
                "행정안전부/국가재난안전교육원/훈령/중앙민방위방재교육원 위임·전결규정/본문.md"
            )
        );

        let research_xml = "<AdmRulService><행정규칙일련번호>124</행정규칙일련번호><행정규칙명>국립재난안전연구원 재난안전연구자문위원회 규정</행정규칙명><행정규칙종류>훈령</행정규칙종류><소관부처명>국립재난안전연구원</소관부처명><상위부처명>국민안전처</상위부처명><담당부서기관명>국립재난안전연구원</담당부서기관명><발령일자>20250619</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let research_rule = parse_admrule(research_xml.as_bytes(), "124").unwrap();
        assert_eq!(research_rule.org_path, ["행정안전부", "국립재난안전연구원"]);
        assert_eq!(
            admrule_path(&research_rule, &mut PathRegistry::new()),
            PathBuf::from(
                "행정안전부/국립재난안전연구원/훈령/국립재난안전연구원 재난안전연구자문위원회 규정/본문.md"
            )
        );
    }

    #[test]
    fn uses_verified_department_root_for_non_current_split_ministries() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>무인도서 관리유형 재지정(변경) 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>국토해양부</소관부처명><상위부처명>국토해양부</상위부처명><담당부서기관명>해양수산부(해양영토과)</담당부서기관명><발령일자>20111104</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(rule.org_path, ["해양수산부"]);
    }

    #[test]
    fn keeps_independent_special_committee_as_root() {
        let xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>10·29 위원회 규칙</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>10.29이태원참사진상규명과재발방지를위한특별조사위원회</소관부처명><상위부처명>행정안전부</상위부처명><담당부서기관명>10·29이태원참사진상규명과재발방지를위한특별조사위원회(운영지원과)</담당부서기관명><발령일자>20260101</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let rule = parse_admrule(xml.as_bytes(), "123").unwrap();
        assert_eq!(
            rule.org_path,
            ["10·29이태원참사진상규명과재발방지를위한특별조사위원회"]
        );
        assert_eq!(
            admrule_path(&rule, &mut PathRegistry::new()),
            PathBuf::from(
                "10·29이태원참사진상규명과재발방지를위한특별조사위원회/_본부/고시/10·29 위원회 규칙/본문.md"
            )
        );
    }

    #[test]
    fn maps_government_affiliated_public_bodies() {
        let landfill_xml = "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>수도권매립지 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>수도권매립지관리공사</소관부처명><상위부처명>정부산하기관및위원회</상위부처명><담당부서기관명>수도권매립지관리공사</담당부서기관명><발령일자>20260101</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let landfill_rule = parse_admrule(landfill_xml.as_bytes(), "123").unwrap();
        assert_eq!(
            landfill_rule.org_path,
            ["기후에너지환경부", "수도권매립지관리공사"]
        );

        let education_xml = "<AdmRulService><행정규칙일련번호>124</행정규칙일련번호><행정규칙명>학점인정 기준</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>평생교육진흥원</소관부처명><상위부처명>정부산하기관및위원회</상위부처명><담당부서기관명>평생교육진흥원</담당부서기관명><발령일자>20260101</발령일자><조문내용>제1조 목적</조문내용></AdmRulService>";
        let education_rule = parse_admrule(education_xml.as_bytes(), "124").unwrap();
        assert_eq!(education_rule.org_path, ["교육부", "국가평생교육진흥원"]);
        assert_eq!(education_rule.original_ministry, "평생교육진흥원");
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
    fn bare_repo_removes_stale_path_when_rule_path_changes() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        fs::create_dir(&cache).unwrap();
        fs::write(
            cache.join("100.xml"),
            "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙ID>ABC</행정규칙ID><행정규칙명>이전 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>행정안전부</소관부처명><발령번호>1</발령번호><발령일자>20240101</발령일자><조문내용>이전 본문</조문내용></AdmRulService>",
        )
        .unwrap();
        fs::write(
            cache.join("200.xml"),
            "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙ID>ABC</행정규칙ID><행정규칙명>새 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>행정안전부</소관부처명><발령번호>2</발령번호><발령일자>20240201</발령일자><조문내용>새 본문</조문내용></AdmRulService>",
        )
        .unwrap();
        let repo = temp.path().join("out.git");

        compile_bare_repo(&cache, &repo, None).unwrap();

        git_ok(&repo, ["fsck", "--full"]);
        assert_eq!(git_stdout(&repo, ["rev-list", "--count", "--all"]), "3");
        let files = git_stdout(&repo, ["ls-tree", "-r", "--name-only", "HEAD"]);
        assert!(files.contains("행정안전부/_본부/고시/새 고시/본문.md"));
        assert!(!files.contains("행정안전부/_본부/고시/이전 고시/본문.md"));
    }

    #[test]
    fn compile_dir_removes_stale_path_when_rule_path_changes() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        fs::create_dir(&cache).unwrap();
        fs::write(
            cache.join("100.xml"),
            "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>이전 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>행정안전부</소관부처명><발령일자>20240101</발령일자><조문내용>이전 본문</조문내용></AdmRulService>",
        )
        .unwrap();
        fs::write(
            cache.join("200.xml"),
            "<AdmRulService><행정규칙일련번호>123</행정규칙일련번호><행정규칙명>새 고시</행정규칙명><행정규칙종류>고시</행정규칙종류><소관부처명>행정안전부</소관부처명><발령일자>20240201</발령일자><조문내용>새 본문</조문내용></AdmRulService>",
        )
        .unwrap();
        let output = temp.path().join("out");

        compile_dir(&cache, &output, None).unwrap();

        assert!(
            output
                .join("행정안전부/_본부/고시/새 고시/본문.md")
                .exists()
        );
        assert!(
            !output
                .join("행정안전부/_본부/고시/이전 고시/본문.md")
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
