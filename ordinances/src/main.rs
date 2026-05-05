//! Compile cached law.go.kr ordinance XML into a Markdown tree.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use git_writer::{BareRepoWriter, GitTimestampKst, PreparedBlob, RepoPathBuf, precompute_blob};
use quick_xml::Reader;
use quick_xml::events::Event;
use time::{Date, Month, PrimitiveDateTime, Time as CivilTime, UtcOffset};
use unicode_normalization::UnicodeNormalization;

const REPOSITORY_README: &[u8] = include_bytes!("../assets/README.md");

/// Command-line interface.
#[derive(Debug, Parser)]
#[command(name = "ordinance-kr-compiler")]
struct Cli {
    /// Directory containing `{자치법규ID}.xml` files.
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

/// Parsed ordinance data.
#[derive(Debug, Clone)]
struct Ordinance {
    /// 자치법규ID.
    id: String,
    /// 자치법규일련번호.
    serial: String,
    /// 자치법규명.
    name: String,
    /// 자치법규종류.
    ordinance_type: String,
    /// 지자체기관명.
    jurisdiction: String,
    /// 공포일자 raw.
    prom_date_raw: String,
    /// 공포번호.
    prom_no: String,
    /// 시행일자 raw.
    effective_date_raw: String,
    /// 제개정구분.
    amendment: String,
    /// 자치법규분야.
    field: String,
    /// 담당부서.
    department: String,
    /// Body text.
    body: String,
    /// Parsed article-like units.
    articles: Vec<Article>,
    /// Addenda text blocks.
    addenda: Vec<String>,
    /// Attachment links parsed from 별표 blocks.
    attachments: Vec<Attachment>,
}

/// Parsed ordinance article unit.
#[derive(Debug, Clone)]
struct Article {
    no: String,
    title: String,
    content: String,
}

/// Parsed ordinance 별표 attachment link.
#[derive(Debug, Clone)]
struct Attachment {
    bylaw_no: String,
    branch_no: String,
    kind: String,
    title: String,
    file_type: String,
    file_link: String,
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
    let entries = render_ordinance_entries(cache_dir, limit)?;
    if entries.is_empty() {
        anyhow::bail!(
            "no valid ordinance XML files found under {}",
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
    repo.finish()?;
    eprintln!("committed {} ordinance markdown files", entries.len());
    Ok(())
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

fn render_ordinance_entries(cache_dir: &Path, limit: Option<usize>) -> Result<Vec<ImportEntry>> {
    let mut files = read_xml_files(cache_dir)?;
    if let Some(limit) = limit {
        files.truncate(limit);
    }
    let mut registry = PathRegistry::new();
    let mut entries = Vec::with_capacity(files.len());
    let mut skipped = 0usize;
    for path in files {
        let raw = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let ordinance = match parse_ordinance(
            &raw,
            path.file_stem().and_then(|s| s.to_str()).unwrap_or(""),
        ) {
            Ok(ordinance) => ordinance,
            Err(err) => {
                skipped += 1;
                eprintln!(
                    "skipping unparsable ordinance XML {}: {err:#}",
                    path.display()
                );
                continue;
            }
        };
        if !matches!(
            ordinance.ordinance_type.as_str(),
            "조례" | "규칙" | "훈령" | "예규" | "고시" | "의회규칙"
        ) {
            skipped += 1;
            continue;
        }
        let rel = ordinance_path(&ordinance, &mut registry);
        entries.push(ImportEntry {
            path: rel.to_string_lossy().replace('\\', "/"),
            previous_path: None,
            identity: ordinance.id.clone(),
            content: render_markdown(&ordinance).into_bytes(),
            message: ordinance_commit_message(&ordinance),
            timestamp: commit_timestamp(&ordinance.prom_date_raw)?,
            sort_date: compact_date_or_epoch(&ordinance.prom_date_raw),
            sort_id: ordinance.id.parse::<u64>().unwrap_or(u64::MAX),
        });
    }
    entries.sort_by(|a, b| {
        a.sort_date
            .cmp(&b.sort_date)
            .then_with(|| a.sort_id.cmp(&b.sort_id))
            .then_with(|| a.path.cmp(&b.path))
    });
    assign_previous_paths(&mut entries);
    eprintln!(
        "prepared {} ordinance markdown files; skipped {skipped}",
        entries.len()
    );
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

fn ordinance_commit_message(ordinance: &Ordinance) -> String {
    format!(
        "{}: {} ({})\n\n자치법규ID: {}\n자치법규일련번호: {}",
        ordinance.ordinance_type,
        ordinance.name,
        ordinance.jurisdiction,
        ordinance.id,
        ordinance.serial
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

/// Compile every XML file under `cache_dir`.
fn compile_dir(cache_dir: &Path, output: &Path, limit: Option<usize>) -> Result<()> {
    fs::create_dir_all(output).with_context(|| format!("failed to create {}", output.display()))?;
    fs::write(output.join("README.md"), REPOSITORY_README)?;
    let entries = render_ordinance_entries(cache_dir, limit)?;
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
    eprintln!("written {} ordinance markdown files", entries.len());
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

/// Parse a cached XML document.
fn parse_ordinance(raw: &[u8], fallback_id: &str) -> Result<Ordinance> {
    let fields = tag_texts(raw)?;
    let id = first(&fields, &["자치법규ID"])
        .unwrap_or(fallback_id)
        .to_string();
    let attachments = collect_attachments(raw)?;
    Ok(Ordinance {
        id,
        serial: first(&fields, &["자치법규일련번호"])
            .unwrap_or("")
            .to_string(),
        name: text_value(first(&fields, &["자치법규명"]).unwrap_or("")),
        ordinance_type: normalize_type(first(&fields, &["자치법규종류"]).unwrap_or("")),
        jurisdiction: text_value(first(&fields, &["지자체기관명"]).unwrap_or("")),
        prom_date_raw: first(&fields, &["공포일자"]).unwrap_or("").to_string(),
        prom_no: first(&fields, &["공포번호"]).unwrap_or("").to_string(),
        effective_date_raw: first(&fields, &["시행일자"]).unwrap_or("").to_string(),
        amendment: text_value(first(&fields, &["제개정구분명", "제개정구분"]).unwrap_or("")),
        field: text_value(first(&fields, &["자치법규분야명"]).unwrap_or("")),
        department: text_value(first(&fields, &["담당부서명"]).unwrap_or("")),
        body: collect_body(&fields, &["조문내용", "조내용", "본문", "내용"]),
        articles: collect_articles(&fields),
        addenda: fields
            .get("부칙내용")
            .map(|values| values.iter().map(|value| nfc(value)).collect())
            .unwrap_or_default(),
        attachments,
    })
}

/// Extract text values by tag.
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

/// Return first available field.
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

/// Collect article vectors by positional index. This matches the simple
/// `조문단위` shape handled by the Python converter's shared article renderer.
fn collect_articles(fields: &BTreeMap<String, Vec<String>>) -> Vec<Article> {
    let numbers = fields.get("조문번호").cloned().unwrap_or_default();
    let titles = fields
        .get("조문제목")
        .or_else(|| fields.get("조제목"))
        .cloned()
        .unwrap_or_default();
    let contents = fields
        .get("조문내용")
        .or_else(|| fields.get("조내용"))
        .cloned()
        .unwrap_or_default();
    contents
        .iter()
        .enumerate()
        .map(|(idx, content)| Article {
            no: numbers
                .get(idx)
                .map(|value| normalize_article_number(value))
                .unwrap_or_default(),
            title: titles.get(idx).cloned().unwrap_or_default(),
            content: nfc(content),
        })
        .collect()
}

fn collect_attachments(raw: &[u8]) -> Result<Vec<Attachment>> {
    let mut reader = Reader::from_reader(raw);
    reader.config_mut().trim_text(true);
    let mut in_attachment = false;
    let mut current = String::new();
    let mut fields: BTreeMap<String, String> = BTreeMap::new();
    let mut attachments = Vec::new();
    loop {
        match reader.read_event()? {
            Event::Start(event) => {
                let name = String::from_utf8_lossy(event.name().as_ref()).to_string();
                if name == "별표단위" {
                    in_attachment = true;
                    fields.clear();
                    current.clear();
                } else if in_attachment {
                    current = name;
                }
            }
            Event::Text(text) if in_attachment && !current.is_empty() => {
                let value = text.decode()?.trim().to_string();
                if !value.is_empty() {
                    fields.entry(current.clone()).or_insert(value);
                }
            }
            Event::CData(text) if in_attachment && !current.is_empty() => {
                let value = text.decode()?.trim().to_string();
                if !value.is_empty() {
                    fields.entry(current.clone()).or_insert(value);
                }
            }
            Event::End(event) => {
                let name = String::from_utf8_lossy(event.name().as_ref()).to_string();
                if name == "별표단위" {
                    if let Some(attachment) = attachment_from_fields(&fields) {
                        attachments.push(attachment);
                    }
                    in_attachment = false;
                    current.clear();
                    fields.clear();
                } else if in_attachment {
                    current.clear();
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(attachments)
}

fn attachment_from_fields(fields: &BTreeMap<String, String>) -> Option<Attachment> {
    let file_link = nfc(fields.get("별표첨부파일명")?).trim().to_string();
    if file_link.is_empty() {
        return None;
    }
    let kind = attachment_field(fields, "별표구분");
    Some(Attachment {
        bylaw_no: attachment_field(fields, "별표번호"),
        branch_no: attachment_field(fields, "별표가지번호"),
        kind: if kind.is_empty() {
            "별표".to_string()
        } else {
            kind
        },
        title: attachment_field(fields, "별표제목"),
        file_type: attachment_field(fields, "별표첨부파일구분").to_lowercase(),
        file_link,
    })
}

fn attachment_field(fields: &BTreeMap<String, String>, key: &str) -> String {
    fields
        .get(key)
        .map(|value| text_value(value))
        .unwrap_or_default()
}

fn normalize_article_number(value: &str) -> String {
    let raw = value.trim();
    if let Ok(number) = raw.parse::<usize>() {
        if number > 0 && number % 100 == 0 {
            return (number / 100).to_string();
        }
        return number.to_string();
    }
    raw.to_string()
}

/// Normalize ordinance type code to label.
fn normalize_type(value: &str) -> String {
    match value {
        "C0001" => "조례",
        "C0002" => "규칙",
        "C0003" => "훈령",
        "C0004" => "예규",
        "C0006" => "기타",
        "C0010" => "고시",
        "C0011" => "의회규칙",
        other => other,
    }
    .to_string()
}

/// NFC-normalize.
fn nfc(value: &str) -> String {
    value.nfc().collect::<String>()
}

fn text_value(value: &str) -> String {
    nfc(value).split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Safe path component.
fn safe_path_part(value: &str) -> String {
    let mut text = text_value(value)
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

/// Split jurisdiction into `(광역, 기초)`.
fn split_jurisdiction(raw: &str) -> Result<(String, String)> {
    const GWANGYEOK: [&str; 18] = [
        "서울특별시",
        "부산광역시",
        "대구광역시",
        "인천광역시",
        "광주광역시",
        "대전광역시",
        "울산광역시",
        "세종특별자치시",
        "강원특별자치도",
        "전북특별자치도",
        "제주특별자치도",
        "충청북도",
        "충청남도",
        "전라남도",
        "경상북도",
        "경상남도",
        "경기도",
        "충청광역연합",
    ];
    let text = nfc(raw)
        .replace("제주도교육청", "제주특별자치도교육청")
        .replace("강원도", "강원특별자치도")
        .replace("전라북도", "전북특별자치도")
        .replace("제주도", "제주특별자치도")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    for gwangyeok in GWANGYEOK {
        if let Some(rest) = text.strip_prefix(gwangyeok) {
            let rest = rest.trim();
            if rest.is_empty() {
                return Ok((gwangyeok.to_string(), "_본청".to_string()));
            }
            if rest.ends_with("교육청") {
                return Ok((gwangyeok.to_string(), "_교육청".to_string()));
            }
            return Ok((gwangyeok.to_string(), rest.to_string()));
        }
    }
    anyhow::bail!("unknown jurisdiction: {raw}")
}

/// Compute repository path.
fn ordinance_path(ordinance: &Ordinance, registry: &mut PathRegistry) -> PathBuf {
    let (gwangyeok, gicho) = split_jurisdiction(&ordinance.jurisdiction)
        .unwrap_or_else(|_| ("_미상".to_string(), safe_path_part(&ordinance.jurisdiction)));
    let gwangyeok = safe_path_part(&gwangyeok);
    let gicho = safe_path_part(&gicho);
    let ordinance_type = safe_path_part(&ordinance.ordinance_type);
    let name = safe_path_part(&ordinance.name);
    let base = format!("{gwangyeok}/{gicho}/{ordinance_type}/{name}/본문.md");
    if claim_path(registry, &base, &ordinance.id) {
        return PathBuf::from(base);
    }
    let candidates = [
        safe_path_part(&ordinance.prom_no),
        safe_path_part(&ordinance.id),
        safe_path_part(&format!("{}_{}", ordinance.prom_no, ordinance.id)),
    ];
    for suffix in candidates {
        let suffixed = format!("{gwangyeok}/{gicho}/{ordinance_type}/{name}_{suffix}/본문.md");
        if claim_path(registry, &suffixed, &ordinance.id) {
            return PathBuf::from(suffixed);
        }
    }
    let mut idx = 2usize;
    loop {
        let suffixed = format!(
            "{gwangyeok}/{gicho}/{ordinance_type}/{name}_{}_{idx}/본문.md",
            ordinance.id
        );
        if claim_path(registry, &suffixed, &ordinance.id) {
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

/// Convert compact dates to ISO dates.
fn format_date(raw: &str) -> String {
    let digits = raw.replace(['.', '-'], "");
    if is_valid_compact_date(&digits) {
        format!("{}-{}-{}", &digits[..4], &digits[4..6], &digits[6..8])
    } else {
        raw.to_string()
    }
}

fn promulgation_date(raw: &str) -> (String, bool) {
    if is_epoch_clamped(raw) {
        ("1970-01-01".to_string(), true)
    } else {
        (format_date(raw), false)
    }
}

fn is_epoch_clamped(raw: &str) -> bool {
    let digits = raw.replace(['.', '-'], "");
    digits.len() == 8
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && (!is_valid_compact_date(&digits) || digits.as_str() < "19700101")
}

/// Render article Markdown compatible with the Python converter for flat
/// article bodies.
fn render_articles(articles: &[Article]) -> String {
    let mut parts = Vec::new();
    for article in articles {
        let title_suffix = if article.title.is_empty() {
            String::new()
        } else {
            format!(" ({})", article.title)
        };
        parts.push(format!("##### 제{}조{}", article.no, title_suffix));
        let stripped = strip_article_prefix(&article.content, &article.no, &article.title);
        if !stripped.is_empty() {
            parts.push(stripped);
        }
    }
    parts.join("\n\n")
}

fn strip_article_prefix(content: &str, no: &str, title: &str) -> String {
    if !no.is_empty() && !title.is_empty() {
        let prefix = format!("제{}조({})", no, title);
        if let Some(rest) = content.strip_prefix(&prefix) {
            return rest.trim().to_string();
        }
    }
    content.trim().to_string()
}

fn public_source_url(ordinance: &Ordinance) -> String {
    let compact_name = ordinance.name.replace(' ', "");
    if compact_name.is_empty() {
        String::new()
    } else {
        format!("https://www.law.go.kr/자치법규/{compact_name}")
    }
}

fn render_attachments_yaml(attachments: &[Attachment]) -> String {
    if attachments.is_empty() {
        return "첨부파일: []\n".to_string();
    }
    let mut out = String::from("첨부파일:\n");
    for attachment in attachments {
        out.push_str(&format!(
            "  - 별표번호: {}\n    별표가지번호: {}\n    별표구분: {}\n    제목: {}\n    파일형식: {}\n    파일링크: {}\n",
            yaml_string(&attachment.bylaw_no),
            yaml_string(&attachment.branch_no),
            yaml_string(&attachment.kind),
            yaml_string(&attachment.title),
            yaml_string(&attachment.file_type),
            yaml_string(&attachment.file_link),
        ));
    }
    out
}

/// Render Markdown.
fn render_markdown(ordinance: &Ordinance) -> String {
    let (gwangyeok, gicho) = split_jurisdiction(&ordinance.jurisdiction)
        .unwrap_or_else(|_| ("_미상".to_string(), safe_path_part(&ordinance.jurisdiction)));
    let articles = render_articles(&ordinance.articles);
    let mut body_text = if !articles.trim().is_empty() {
        articles
    } else {
        ordinance.body.trim().to_string()
    };
    for addendum in &ordinance.addenda {
        let content = addendum.trim();
        if !content.is_empty() {
            if !body_text.trim().is_empty() {
                body_text.push_str("\n\n");
            }
            body_text.push_str("## 부칙\n\n");
            body_text.push_str(content);
        }
    }
    let body = if body_text.trim().is_empty() {
        "본문은 첨부파일 또는 원문을 참조하세요.".to_string()
    } else {
        body_text.trim().to_string()
    };
    let body_source = if body_text.trim().is_empty() {
        "parsing-failed"
    } else {
        "api-text"
    };
    let attachments_yaml = render_attachments_yaml(&ordinance.attachments);
    let (promulgation_date, promulgation_date_clamped) =
        promulgation_date(&ordinance.prom_date_raw);
    format!(
        "---\n자치법규ID: {}\n자치법규일련번호: {}\n자치법규명: {}\n자치법규종류: {}\n지자체기관명: {}\n지자체구분:\n  광역: {}\n  기초: {}\n공포일자: {}\n공포번호: {}\n시행일자: {}\n제개정구분: {}\n자치법규분야: {}\n담당부서: {}\n본문출처: {}\n출처: {}\n{}공포일자보정: {}\n공포일자원문: {}\n---\n\n# {}\n\n{}\n",
        yaml_string(&ordinance.id),
        yaml_string(&ordinance.serial),
        yaml_string(&ordinance.name),
        yaml_string(&ordinance.ordinance_type),
        yaml_string(&ordinance.jurisdiction),
        yaml_string(&gwangyeok),
        yaml_string(&gicho),
        promulgation_date,
        yaml_string(&ordinance.prom_no),
        yaml_string(&format_date(&ordinance.effective_date_raw)),
        yaml_string(&ordinance.amendment),
        yaml_string(&ordinance.field),
        yaml_string(&ordinance.department),
        yaml_string(body_source),
        yaml_string(&public_source_url(ordinance)),
        attachments_yaml,
        promulgation_date_clamped,
        yaml_string(&ordinance.prom_date_raw),
        ordinance.name,
        body
    )
}

fn yaml_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;

    #[test]
    fn parses_code_type_and_jurisdiction() {
        let xml = "<Ordin><자치법규ID>2000111</자치법규ID><자치법규명>서울특별시 테스트 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><공포일자>20240504</공포일자><조문내용>제1조 목적</조문내용></Ordin>";
        let ordinance = parse_ordinance(xml.as_bytes(), "2000111").unwrap();
        assert_eq!(ordinance.ordinance_type, "조례");
        assert!(render_markdown(&ordinance).contains("기초: '_본청'"));
        assert!(!render_markdown(&ordinance).contains("source_url:"));
    }

    #[test]
    fn path_registry_reuses_path_for_same_ordinance_id_revisions() {
        let first = parse_ordinance(
            "<Ordin><자치법규ID>2000111</자치법규ID><자치법규명>서울특별시 테스트 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><공포번호>100</공포번호></Ordin>".as_bytes(),
            "2000111",
        )
        .unwrap();
        let second = parse_ordinance(
            "<Ordin><자치법규ID>2000111</자치법규ID><자치법규명>서울특별시 테스트 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><공포번호>101</공포번호></Ordin>".as_bytes(),
            "2000111",
        )
        .unwrap();
        let other = parse_ordinance(
            "<Ordin><자치법규ID>2000222</자치법규ID><자치법규명>서울특별시 테스트 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><공포번호>102</공포번호></Ordin>".as_bytes(),
            "2000222",
        )
        .unwrap();
        let mut registry = PathRegistry::new();
        let base = PathBuf::from("서울특별시/_본청/조례/서울특별시 테스트 조례/본문.md");
        assert_eq!(ordinance_path(&first, &mut registry), base);
        assert_eq!(ordinance_path(&second, &mut registry), base);
        assert_eq!(
            ordinance_path(&other, &mut registry),
            PathBuf::from("서울특별시/_본청/조례/서울특별시 테스트 조례_102/본문.md")
        );
    }

    #[test]
    fn parses_observed_article_tags_and_extra_type_codes() {
        let xml = "<Ordin><자치법규ID>2240395</자치법규ID><자치법규명>서울특별시 기준</자치법규명><자치법규종류>C0010</자치법규종류><지자체기관명>제주도교육청</지자체기관명><조><조문번호>000100</조문번호><조제목>목적</조제목><조내용>제1조(목적) 내용</조내용></조></Ordin>";
        let ordinance = parse_ordinance(xml.as_bytes(), "2240395").unwrap();
        assert_eq!(ordinance.ordinance_type, "고시");
        assert_eq!(
            split_jurisdiction(&ordinance.jurisdiction).unwrap(),
            ("제주특별자치도".to_string(), "_교육청".to_string())
        );
        assert!(render_markdown(&ordinance).contains("##### 제1조 (목적)"));
    }

    #[test]
    fn normalizes_historical_jurisdiction_names() {
        assert_eq!(
            split_jurisdiction("강원도 춘천시").unwrap(),
            ("강원특별자치도".to_string(), "춘천시".to_string())
        );
        assert_eq!(
            split_jurisdiction("전라북도 전주시").unwrap(),
            ("전북특별자치도".to_string(), "전주시".to_string())
        );
        assert_eq!(
            split_jurisdiction("제주도 제주시").unwrap(),
            ("제주특별자치도".to_string(), "제주시".to_string())
        );
    }

    #[test]
    fn renders_addenda_when_articles_are_empty() {
        let xml = "<Ordin><자치법규ID>1</자치법규ID><자치법규명>부칙 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><부칙><부칙내용>이 조례는 공포한 날부터 시행한다.</부칙내용></부칙></Ordin>";
        let ordinance = parse_ordinance(xml.as_bytes(), "1").unwrap();
        let markdown = render_markdown(&ordinance);
        assert!(markdown.contains("본문출처: 'api-text'"));
        assert!(markdown.contains("## 부칙\n\n이 조례는 공포한 날부터 시행한다."));
    }

    #[test]
    fn renders_attachment_links() {
        let xml = "<Ordin><자치법규ID>1</자치법규ID><자치법규명>첨부 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><조문내용>제1조 목적</조문내용><별표><별표단위><별표번호>0001</별표번호><별표가지번호>00</별표가지번호><별표구분>서식</별표구분><별표제목><![CDATA[[별지 제1호서식] 신청서]]></별표제목><별표첨부파일구분>hwp</별표첨부파일구분><별표첨부파일명><![CDATA[http://www.law.go.kr/flDownload.do?gubun=ELIS&flSeq=1&flNm=test]]></별표첨부파일명></별표단위></별표></Ordin>";
        let ordinance = parse_ordinance(xml.as_bytes(), "1").unwrap();
        let markdown = render_markdown(&ordinance);
        assert!(markdown.contains("첨부파일:\n  - 별표번호: '0001'"));
        assert!(markdown.contains("별표구분: '서식'"));
        assert!(markdown.contains("제목: '[별지 제1호서식] 신청서'"));
        assert!(markdown.contains(
            "파일링크: 'http://www.law.go.kr/flDownload.do?gubun=ELIS&flSeq=1&flNm=test'"
        ));
    }

    #[test]
    fn renders_neutral_stub_when_body_is_missing() {
        let xml = "<Ordin><자치법규ID>1</자치법규ID><자치법규명>첨부 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><별표><별표단위><별표첨부파일구분>pdf</별표첨부파일구분><별표첨부파일명>https://example.test/file.pdf</별표첨부파일명></별표단위></별표></Ordin>";
        let ordinance = parse_ordinance(xml.as_bytes(), "1").unwrap();
        let markdown = render_markdown(&ordinance);
        assert!(markdown.contains("본문은 첨부파일 또는 원문을 참조하세요."));
        assert!(!markdown.contains("첨부파일(HWP)"));
    }

    #[test]
    fn invalid_compact_dates_fall_back_to_epoch_for_commit_timestamp() {
        assert_eq!(compact_date_or_epoch("20240229"), "20240229");
        assert_eq!(compact_date_or_epoch("20240231"), "19700101");
        assert_eq!(compact_date_or_epoch("20241301"), "19700101");
        assert_eq!(format_date("20240231"), "20240231");
        assert_eq!(
            promulgation_date("20240231"),
            ("1970-01-01".to_string(), true)
        );
        assert_eq!(compact_date_or_epoch("19691231"), "19700101");
        assert_eq!(
            commit_timestamp("20240231").unwrap(),
            commit_timestamp("19700101").unwrap()
        );
    }

    #[test]
    fn parses_cdata_fields() {
        let xml = "<Ordin><자치법규ID>1</자치법규ID><자치법규명><![CDATA[CDATA 조례]]></자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><조문내용><![CDATA[제1조 목적]]></조문내용></Ordin>";
        let ordinance = parse_ordinance(xml.as_bytes(), "1").unwrap();
        assert_eq!(ordinance.name, "CDATA 조례");
        assert_eq!(ordinance.body, "제1조 목적");
    }

    #[test]
    fn normalizes_and_quotes_yaml_sensitive_name() {
        let xml = "<Ordin><자치법규ID>1</자치법규ID><자치법규명>서울특별시 옥외행사 안전관리에\n등에 관한 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><조문내용>제1조 목적</조문내용></Ordin>";
        let ordinance = parse_ordinance(xml.as_bytes(), "1").unwrap();
        assert_eq!(
            ordinance.name,
            "서울특별시 옥외행사 안전관리에 등에 관한 조례"
        );
        assert!(
            render_markdown(&ordinance)
                .contains("자치법규명: '서울특별시 옥외행사 안전관리에 등에 관한 조례'")
        );
    }

    #[test]
    fn bare_repo_uses_main_and_one_commit_per_ordinance() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        fs::create_dir(&cache).unwrap();
        fs::write(
            cache.join("2000111.xml"),
            "<Ordin><자치법규ID>2000111</자치법규ID><자치법규일련번호>1</자치법규일련번호><자치법규명>서울특별시 테스트 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><공포일자>20240504</공포일자><공포번호>1</공포번호><조문내용>제1조 목적</조문내용></Ordin>",
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
                .join("서울특별시/_본청/조례/서울특별시 테스트 조례/본문.md")
                .exists()
        );
    }

    #[test]
    fn bare_repo_removes_stale_path_when_ordinance_path_changes() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        fs::create_dir(&cache).unwrap();
        fs::write(
            cache.join("100.xml"),
            "<Ordin><자치법규ID>2000111</자치법규ID><자치법규일련번호>1</자치법규일련번호><자치법규명>이전 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><공포일자>20240101</공포일자><공포번호>1</공포번호><조문내용>이전 본문</조문내용></Ordin>",
        )
        .unwrap();
        fs::write(
            cache.join("200.xml"),
            "<Ordin><자치법규ID>2000111</자치법규ID><자치법규일련번호>2</자치법규일련번호><자치법규명>새 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><공포일자>20240201</공포일자><공포번호>2</공포번호><조문내용>새 본문</조문내용></Ordin>",
        )
        .unwrap();
        let repo = temp.path().join("out.git");

        compile_bare_repo(&cache, &repo, None).unwrap();

        git_ok(&repo, ["fsck", "--full"]);
        assert_eq!(git_stdout(&repo, ["rev-list", "--count", "--all"]), "3");
        let files = git_stdout(&repo, ["ls-tree", "-r", "--name-only", "HEAD"]);
        assert!(files.contains("서울특별시/_본청/조례/새 조례/본문.md"));
        assert!(!files.contains("서울특별시/_본청/조례/이전 조례/본문.md"));
    }

    #[test]
    fn compile_dir_removes_stale_path_when_ordinance_path_changes() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        fs::create_dir(&cache).unwrap();
        fs::write(
            cache.join("100.xml"),
            "<Ordin><자치법규ID>2000111</자치법규ID><자치법규일련번호>1</자치법규일련번호><자치법규명>이전 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><공포일자>20240101</공포일자><공포번호>1</공포번호><조문내용>이전 본문</조문내용></Ordin>",
        )
        .unwrap();
        fs::write(
            cache.join("200.xml"),
            "<Ordin><자치법규ID>2000111</자치법규ID><자치법규일련번호>2</자치법규일련번호><자치법규명>새 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><공포일자>20240201</공포일자><공포번호>2</공포번호><조문내용>새 본문</조문내용></Ordin>",
        )
        .unwrap();
        let output = temp.path().join("out");

        compile_dir(&cache, &output, None).unwrap();

        assert!(
            output
                .join("서울특별시/_본청/조례/새 조례/본문.md")
                .exists()
        );
        assert!(
            !output
                .join("서울특별시/_본청/조례/이전 조례/본문.md")
                .exists()
        );
    }

    #[test]
    fn compile_dir_applies_revisions_in_promulgation_order() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        fs::create_dir(&cache).unwrap();
        fs::write(
            cache.join("100.xml"),
            "<Ordin><자치법규ID>2000111</자치법규ID><자치법규일련번호>2</자치법규일련번호><자치법규명>서울특별시 테스트 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><공포일자>20240504</공포일자><공포번호>2</공포번호><조문내용>최신 본문</조문내용></Ordin>",
        )
        .unwrap();
        fs::write(
            cache.join("200.xml"),
            "<Ordin><자치법규ID>2000111</자치법규ID><자치법규일련번호>1</자치법규일련번호><자치법규명>서울특별시 테스트 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><공포일자>20240101</공포일자><공포번호>1</공포번호><조문내용>이전 본문</조문내용></Ordin>",
        )
        .unwrap();
        let output = temp.path().join("out");

        compile_dir(&cache, &output, None).unwrap();

        let markdown =
            fs::read_to_string(output.join("서울특별시/_본청/조례/서울특별시 테스트 조례/본문.md"))
                .unwrap();
        assert!(markdown.contains("공포일자: 2024-05-04"));
        assert!(markdown.contains("공포번호: '2'"));
        assert!(markdown.contains("최신 본문"));
        assert!(!markdown.contains("이전 본문"));
    }

    #[test]
    fn bare_repo_rejects_empty_valid_entries() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        fs::create_dir(&cache).unwrap();
        fs::write(
            cache.join("2000111.xml"),
            "<Ordin><자치법규ID>2000111</자치법규ID><자치법규명>기타</자치법규명><자치법규종류>C0006</자치법규종류></Ordin>",
        )
        .unwrap();
        let repo = temp.path().join("out.git");
        let error = compile_bare_repo(&cache, &repo, None).unwrap_err();
        assert!(error.to_string().contains("no valid ordinance XML"));
        assert!(!repo.exists());
    }

    #[test]
    fn bare_repo_skips_unparsable_xml_without_aborting_valid_entries() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        fs::create_dir(&cache).unwrap();
        fs::write(cache.join("bad.xml"), "<Ordin><").unwrap();
        fs::write(
            cache.join("2000111.xml"),
            "<Ordin><자치법규ID>2000111</자치법규ID><자치법규일련번호>1</자치법규일련번호><자치법규명>서울특별시 테스트 조례</자치법규명><자치법규종류>C0001</자치법규종류><지자체기관명>서울특별시</지자체기관명><공포일자>20240504</공포일자><공포번호>1</공포번호><조문내용>제1조 목적</조문내용></Ordin>",
        )
        .unwrap();
        let repo = temp.path().join("out.git");

        compile_bare_repo(&cache, &repo, None).unwrap();
        git_ok(&repo, ["fsck", "--full"]);
        assert_eq!(git_stdout(&repo, ["rev-list", "--count", "--all"]), "2");
    }

    #[test]
    fn bare_repo_preserves_existing_output_when_no_valid_entries_exist() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        fs::create_dir(&cache).unwrap();
        fs::write(cache.join("bad.xml"), "<Ordin><").unwrap();
        let repo = temp.path().join("out.git");
        fs::create_dir(&repo).unwrap();
        fs::write(repo.join("marker"), "keep").unwrap();

        let error = compile_bare_repo(&cache, &repo, None).unwrap_err();
        assert!(error.to_string().contains("no valid ordinance XML"));
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
