//! Builds a fresh bare Git repository from cached law.go.kr precedent XML files.
//!
//! The compiler reads an existing `.cache/precedent/` tree in two passes: metadata is collected
//! and stably sorted first, then each XML document is fully parsed, rendered to Markdown, and
//! committed into a new bare repo.
#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

/// Renders parsed precedent data into Markdown and commit messages.
mod render;
/// Parses cached XML documents into metadata and body structures.
mod xml_parser;

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use git_writer::{BareRepoWriter, GitTimestampKst, RepoPathBuf, precompute_blob};
use rayon::prelude::*;
use time::{Date, Month, PrimitiveDateTime, Time as CivilTime, UtcOffset};

use crate::render::{
    PathRegistry, build_commit_message, format_judgment_date, get_precedent_path,
    legacy_get_precedent_path, precedent_to_markdown,
};
use crate::xml_parser::{
    PrecedentDetail, PrecedentMetadata, parse_metadata_only, parse_precedent_body,
};

/// Bundled README payload for the synthetic initial commit.
const REPOSITORY_README: &[u8] = include_bytes!("../assets/README.md");

/// Root `.gitignore` payload matching the generated `precedent-kr` data repository.
const REPOSITORY_GITIGNORE: &[u8] = b"metadata.json\nstats.json\n";

/// Global allocator tuned for high-throughput allocation-heavy pack generation.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Command-line interface for one-shot cache compilation.
#[derive(Debug, Parser)]
#[command(name = "precedent-kr-compiler")]
#[command(about = "Compile cached law.go.kr precedent XML into a fresh bare Git repository")]
struct Cli {
    /// Path to the existing `.cache/precedent/` directory (or any directory of `{serial}.xml`).
    cache_dir: PathBuf,

    /// Output bare repository path.
    #[arg(short = 'o', long = "output", default_value = "output.git")]
    output: PathBuf,

    /// Optional path to write a `legacy-paths.json` mapping the legacy single-key
    /// precedent-kr filenames to the new composite-key filenames. Used by Phase 4
    /// diff harness, cli-tools fallback lookup, and legalize-web redirect table.
    #[arg(long = "emit-legacy-paths")]
    emit_legacy_paths: Option<PathBuf>,

    /// Optional path to the existing `precedent-kr` working tree. When provided,
    /// records whose computed legacy `old_path` does NOT exist in this tree get
    /// `old_path: null` in the emission (per plan §3 Phase 3 schema).
    #[arg(long = "legacy-precedent-root")]
    legacy_precedent_root: Option<PathBuf>,
}

/// Pass-1 planning record for one XML document.
#[derive(Debug, Clone)]
struct PlannedEntry {
    /// Precedent serial used for file lookup and stable ordering.
    serial: String,
    /// Final repository path assigned after collision handling.
    path: RepoPathBuf,
    /// Legacy single-key path that this precedent occupies in the existing
    /// `precedent-kr` repo (used only for `--emit-legacy-paths`).
    legacy_path: RepoPathBuf,
    /// Metadata collected during the cheap planning pass.
    metadata: PrecedentMetadata,
}

/// Fully rendered pass-2 output that is ready to commit.
struct Rendered {
    /// Destination repository path for the Markdown file.
    path: RepoPathBuf,
    /// Final Markdown bytes stored in Git.
    markdown: Vec<u8>,
    /// Canonical Git blob id for the rendered Markdown.
    blob_sha: [u8; 20],
    /// Precompressed PACK payload for the rendered Markdown blob.
    compressed_blob: Vec<u8>,
    /// Commit message for this revision.
    message: String,
    /// Deterministic KST commit timestamp derived during pass 2.
    time: GitTimestampKst,
}

/// Number of entries rendered per worker batch before the writer catches up.
const CHUNK_SIZE: usize = 500;

/// 2026-03-30 12:00:00 KST (UTC+9) = 2026-03-30 03:00:00 UTC for the synthetic initial commits.
const INITIAL_COMMIT_EPOCH: i64 = 1_774_839_600;

/// Parses CLI flags and runs the compiler.
fn main() -> Result<()> {
    let cli = Cli::parse();
    run(cli)
}

/// Executes the full two-pass cache-to-Git compilation pipeline.
fn run(cli: Cli) -> Result<()> {
    let cache_dir = cli.cache_dir.clone();
    if !cache_dir.is_dir() {
        anyhow::bail!("cache directory not found: {}", cache_dir.display());
    }

    //
    // Pass 1 only touches metadata so every later full parse follows one stable order.
    //
    eprintln!("1. Scanning cache metadata");
    let entries = {
        let files = read_sorted_files(&cache_dir, "xml")?;
        let parsed = files
            .par_iter()
            .map(|path| -> Result<Option<PlannedEntry>> {
                let serial = path
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .map(ToOwned::to_owned)
                    .with_context(|| format!("invalid file name: {}", path.display()))?;
                let xml =
                    fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
                match parse_metadata_only(&xml, &serial) {
                    Ok(Some(metadata)) => Ok(Some(PlannedEntry {
                        serial,
                        path: RepoPathBuf::root_file(String::new()),
                        legacy_path: RepoPathBuf::root_file(String::new()),
                        metadata,
                    })),
                    Ok(None) => {
                        eprintln!("warning: skipping non-precedent XML {}", path.display());
                        Ok(None)
                    }
                    Err(error) => {
                        eprintln!(
                            "warning: skipping unparsable cache file {}: {error:#}",
                            path.display()
                        );
                        Ok(None)
                    }
                }
            })
            .collect::<Vec<_>>();

        let mut entries = Vec::with_capacity(files.len());
        for planned in parsed {
            if let Some(planned) = planned? {
                entries.push(planned);
            }
        }

        //
        // Sort by 판례일련번호 lexicographically to match Python's
        // `sorted(PREC_CACHE_DIR.glob("*.xml"))` iteration order in
        // `precedents/import_precedents.py`. This determines which entry wins
        // the clean collision-free path when multiple precedents share a 사건번호.
        //
        entries.sort_by(|left, right| left.serial.cmp(&right.serial));

        let mut registry = PathRegistry::default();
        let mut legacy_registry = PathRegistry::default();
        for entry in &mut entries {
            entry.path = get_precedent_path(&entry.metadata, &mut registry);
            entry.legacy_path = legacy_get_precedent_path(&entry.metadata, &mut legacy_registry);
        }

        //
        // Re-sort by 선고일자 ASC (with serial as a stable tiebreak) so the commit
        // graph follows chronological order. The serial sort above is preserved as
        // the *collision-resolution* order; this re-sort only affects the order in
        // which precedents are appended to the commit chain. Empty dates sort last,
        // matching `precedents/import_precedents.py --git`; sentinel dates such as
        // `00000000` retain their raw lexical order and clamp to the Git epoch in pass 2.
        //
        entries.sort_by(|left, right| {
            judgment_sort_key(&left.metadata.judgment_date)
                .cmp(judgment_sort_key(&right.metadata.judgment_date))
                .then_with(|| left.serial.cmp(&right.serial))
        });

        entries
    };
    if entries.is_empty() {
        anyhow::bail!(
            "no valid precedent XML files found under {}",
            cache_dir.display()
        );
    }

    //
    // Seed the synthetic history commits that always come before precedent revisions.
    //
    eprintln!(
        "2. Writing {} commits to {}",
        entries.len(),
        cli.output.display()
    );
    let mut repo = BareRepoWriter::create(&cli.output)?;
    repo.commit_static(
        &RepoPathBuf::root_file("README.md"),
        REPOSITORY_README,
        "initial commit",
        INITIAL_COMMIT_EPOCH,
    )?;
    eprintln!("  committed README.md");
    repo.commit_static(
        &RepoPathBuf::root_file(".gitignore"),
        REPOSITORY_GITIGNORE,
        "Add generated metadata ignores",
        INITIAL_COMMIT_EPOCH,
    )?;
    eprintln!("  committed .gitignore");

    //
    // Parse/render chunks in parallel while the main thread keeps Git writes ordered.
    //
    let total = entries.len();
    let chunks: Vec<&[PlannedEntry]> = entries.chunks(CHUNK_SIZE).collect();
    let mut pending: Option<Vec<Result<Rendered>>> = None;
    let mut committed = 0usize;

    for (index, chunk) in chunks.iter().enumerate() {
        let cache_dir_for_thread = cache_dir.clone();
        let next = if index + 1 < chunks.len() {
            let next_chunk: Vec<PlannedEntry> = chunks[index + 1].to_vec();
            let next_cache_dir = cache_dir_for_thread.clone();
            Some(std::thread::spawn(move || {
                next_chunk
                    .par_iter()
                    .map(|entry| render_entry(&next_cache_dir, entry))
                    .collect::<Vec<_>>()
            }))
        } else {
            None
        };

        let rendered = if let Some(previous) = pending.take() {
            previous
        } else {
            chunk
                .par_iter()
                .map(|entry| render_entry(&cache_dir, entry))
                .collect::<Vec<_>>()
        };

        for rendered in rendered {
            let rendered = rendered?;
            repo.commit_precedent(
                &rendered.path,
                &rendered.markdown,
                rendered.blob_sha,
                &rendered.compressed_blob,
                &rendered.message,
                rendered.time,
            )?;
            committed += 1;
            if committed.is_multiple_of(500) || committed == total {
                eprintln!("  committed {committed}/{total}");
            }
        }

        if let Some(handle) = next {
            pending = Some(handle.join().expect("render worker panicked"));
        }
    }

    eprintln!("3. Finalizing pack + index");
    repo.finish()?;

    if let Some(emit_path) = cli.emit_legacy_paths.as_ref() {
        write_legacy_paths_json(emit_path, &entries, cli.legacy_precedent_root.as_deref())?;
    }

    Ok(())
}

/// JSON entry written to `legacy-paths.json` (see plan §3 Phase 3 schema).
#[derive(Debug, serde::Serialize)]
struct LegacyPathEntry<'a> {
    /// 판례일련번호 — primary join key for cli-tools / web redirect lookup.
    #[serde(rename = "판례일련번호")]
    serial: &'a str,
    /// Path of the existing precedent-kr file, or `null` for newly added records
    /// (when `--legacy-precedent-root` was passed and the legacy file is absent).
    old_path: Option<String>,
    /// Path of the corresponding file in the new bare repo.
    new_path: String,
}

/// Writes the `legacy-paths.json` mapping required by Phase 4 diff harness,
/// cli-tools fallback lookup, and legalize-web redirect generation.
///
/// All string values are NFC-normalized; entries are sorted by `판례일련번호` ASC
/// (string compare). When `precedent_root` is provided, records whose computed
/// `old_path` doesn't exist on disk get `old_path: null`.
fn write_legacy_paths_json(
    output: &Path,
    entries: &[PlannedEntry],
    precedent_root: Option<&Path>,
) -> Result<()> {
    use unicode_normalization::UnicodeNormalization;

    let mut sorted: Vec<&PlannedEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.serial.cmp(&b.serial));

    let mut payload: Vec<LegacyPathEntry<'_>> = Vec::with_capacity(sorted.len());
    for entry in &sorted {
        let new_path: String = entry.path.to_string().nfc().collect();
        let legacy_str: String = entry.legacy_path.to_string().nfc().collect();
        let old_path = match precedent_root {
            Some(root) => {
                if root.join(&legacy_str).exists() {
                    Some(legacy_str)
                } else {
                    None
                }
            }
            None => Some(legacy_str),
        };
        payload.push(LegacyPathEntry {
            serial: entry.serial.as_str(),
            old_path,
            new_path,
        });
    }

    let bytes = serde_json::to_vec_pretty(&payload)
        .with_context(|| format!("serialize legacy-paths.json for {}", output.display()))?;
    fs::write(output, bytes)
        .with_context(|| format!("write legacy-paths.json to {}", output.display()))?;
    eprintln!(
        "  wrote legacy-paths.json with {} entries to {}",
        payload.len(),
        output.display()
    );
    Ok(())
}

/// Parses, renders, and packages one planned XML entry for pass 2.
fn render_entry(cache_dir: &Path, entry: &PlannedEntry) -> Result<Rendered> {
    let xml_path = cache_dir.join(format!("{}.xml", entry.serial));
    let xml =
        fs::read(&xml_path).with_context(|| format!("failed to read {}", xml_path.display()))?;
    let body = parse_precedent_body(&xml)
        .with_context(|| format!("failed to parse {}", xml_path.display()))?;
    let detail = PrecedentDetail {
        metadata: entry.metadata.clone(),
        body,
    };
    let time = timestamp_from_judgment_date(&detail.metadata.judgment_date)?;

    let markdown = precedent_to_markdown(&detail)?;
    let (blob_sha, compressed_blob) = precompute_blob(&markdown);
    let message = build_commit_message(&detail.metadata);
    Ok(Rendered {
        path: entry.path.clone(),
        markdown,
        blob_sha,
        compressed_blob,
        message,
        time,
    })
}

/// Lists files with the requested extension in deterministic path order.
fn read_sorted_files(dir: &Path, extension: &str) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for item in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let path = item?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some(extension) {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

/// Sort key matching Python `e[0].get("선고일자", "") or "99999999"`.
fn judgment_sort_key(judgment_date: &str) -> &str {
    if judgment_date.is_empty() {
        "99999999"
    } else {
        judgment_date
    }
}

/// Converts a 선고일자 into the deterministic noon-KST commit timestamp.
fn timestamp_from_judgment_date(judgment_date: &str) -> Result<GitTimestampKst> {
    if judgment_date.is_empty() {
        return Ok(GitTimestampKst::from_epoch(0));
    }
    if judgment_date.len() != 8 || !judgment_date.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(GitTimestampKst::from_epoch(0));
    }

    let effective_date = if judgment_date.starts_with("0000") || judgment_date.starts_with("0001") {
        String::from("1970-01-01")
    } else {
        let Some(formatted) = format_judgment_date(judgment_date) else {
            return Ok(GitTimestampKst::from_epoch(0));
        };
        if formatted.as_str() < "1970-01-01" {
            String::from("1970-01-01")
        } else {
            formatted
        }
    };

    let year = effective_date[0..4].parse::<i32>()?;
    let month = Month::try_from(effective_date[5..7].parse::<u8>()?)?;
    let day = effective_date[8..10].parse::<u8>()?;
    let date = Date::from_calendar_date(year, month, day)?;
    let datetime = PrimitiveDateTime::new(date, CivilTime::from_hms(12, 0, 0)?);
    Ok(GitTimestampKst::from_epoch(
        datetime
            .assume_offset(UtcOffset::from_hms(9, 0, 0)?)
            .unix_timestamp(),
    ))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::process::Command;

    use tempfile::TempDir;

    use super::*;

    const SAMPLE_PREC_1: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<PrecService>
  <판례정보일련번호>1001</판례정보일련번호>
  <사건명><![CDATA[손해배상]]></사건명>
  <사건번호><![CDATA[2024가합1]]></사건번호>
  <선고일자>20240101</선고일자>
  <법원명>대법원</법원명>
  <법원종류코드>400201</법원종류코드>
  <사건종류명>민사</사건종류명>
  <판시사항><![CDATA[판시사항 본문]]></판시사항>
  <판결요지><![CDATA[판결요지 본문]]></판결요지>
  <판례내용><![CDATA[판례내용 본문]]></판례내용>
</PrecService>"#;

    const SAMPLE_PREC_2: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<PrecService>
  <판례정보일련번호>1002</판례정보일련번호>
  <사건명><![CDATA[공무집행방해]]></사건명>
  <사건번호><![CDATA[2024도1]]></사건번호>
  <선고일자>20240201</선고일자>
  <법원명>서울고법</법원명>
  <법원종류코드>400202</법원종류코드>
  <사건종류명>형사</사건종류명>
  <판례내용><![CDATA[본문]]></판례내용>
</PrecService>"#;

    const SAMPLE_INVALID_HTML: &str = r#"<!DOCTYPE html>
<html><body>error</body></html>
"#;

    const SAMPLE_INVALID_DATE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<PrecService>
  <판례정보일련번호>1003</판례정보일련번호>
  <사건명><![CDATA[무효확인]]></사건명>
  <사건번호><![CDATA[2024구합1]]></사건번호>
  <선고일자>20241301</선고일자>
  <법원명>대법원</법원명>
  <법원종류코드>400201</법원종류코드>
  <사건종류명>일반행정</사건종류명>
  <판례내용><![CDATA[본문]]></판례내용>
</PrecService>"#;

    fn write_sample(cache_dir: &Path, serial: &str, xml: &str) {
        fs::write(cache_dir.join(format!("{serial}.xml")), xml).unwrap();
    }

    #[test]
    fn end_to_end_builds_bare_repo() {
        let temp = TempDir::new().unwrap();
        let cache_dir = temp.path().join(".cache").join("precedent");
        fs::create_dir_all(&cache_dir).unwrap();
        write_sample(&cache_dir, "1001", SAMPLE_PREC_1);
        write_sample(&cache_dir, "1002", SAMPLE_PREC_2);
        write_sample(&cache_dir, "9999", SAMPLE_INVALID_HTML);

        let output = temp.path().join("output.git");
        run(Cli {
            cache_dir: cache_dir.clone(),
            output: output.clone(),
            emit_legacy_paths: None,
            legacy_precedent_root: None,
        })
        .unwrap();

        git_stdout(&output, ["fsck", "--full"]);
        assert_eq!(
            git_stdout(&output, ["symbolic-ref", "--short", "HEAD"]).trim(),
            "main"
        );
        assert_eq!(
            git_stdout(&output, ["rev-list", "--count", "HEAD"]).trim(),
            "4"
        );

        let tree = git_stdout(
            &output,
            [
                "-c",
                "core.quotePath=false",
                "ls-tree",
                "-r",
                "--name-only",
                "HEAD",
            ],
        );
        let names: Vec<&str> = tree.lines().collect();
        assert!(names.contains(&"README.md"));
        assert!(names.contains(&".gitignore"));
        assert!(names.contains(&"민사/대법원/대법원_2024-01-01_2024가합1.md"));
        assert!(names.contains(&"형사/하급심/서울고등법원_2024-02-01_2024도1.md"));

        let markdown = git_stdout(
            &output,
            ["show", "HEAD:민사/대법원/대법원_2024-01-01_2024가합1.md"],
        );
        assert!(markdown.contains("판례일련번호: '1001'"));
        assert!(markdown.contains("# 손해배상"));
        assert!(markdown.contains("## 판시사항"));
    }

    #[test]
    fn emits_legacy_paths_json_with_missing_old_paths_as_null() {
        let temp = TempDir::new().unwrap();
        let cache_dir = temp.path().join(".cache").join("precedent");
        fs::create_dir_all(&cache_dir).unwrap();
        write_sample(&cache_dir, "1001", SAMPLE_PREC_1);
        write_sample(&cache_dir, "1002", SAMPLE_PREC_2);

        let legacy_root = temp.path().join("legacy");
        let legacy_file = legacy_root.join("민사").join("대법원").join("2024가합1.md");
        fs::create_dir_all(legacy_file.parent().unwrap()).unwrap();
        fs::write(&legacy_file, "legacy").unwrap();

        let legacy_paths = temp.path().join("legacy-paths.json");
        run(Cli {
            cache_dir,
            output: temp.path().join("output.git"),
            emit_legacy_paths: Some(legacy_paths.clone()),
            legacy_precedent_root: Some(legacy_root),
        })
        .unwrap();

        let raw = fs::read_to_string(&legacy_paths).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let entries = payload.as_array().unwrap();
        assert_eq!(entries.len(), 2);

        assert_eq!(entries[0]["판례일련번호"], "1001");
        assert_eq!(entries[0]["old_path"], "민사/대법원/2024가합1.md");
        assert_eq!(
            entries[0]["new_path"],
            "민사/대법원/대법원_2024-01-01_2024가합1.md"
        );

        assert_eq!(entries[1]["판례일련번호"], "1002");
        assert!(entries[1]["old_path"].is_null());
        assert_eq!(
            entries[1]["new_path"],
            "형사/하급심/서울고등법원_2024-02-01_2024도1.md"
        );
    }

    #[test]
    fn empty_judgment_dates_sort_last_like_python_git_import() {
        assert!(judgment_sort_key("00000000") < judgment_sort_key("19700101"));
        assert!(judgment_sort_key("20240101") < judgment_sort_key(""));
    }

    #[test]
    fn malformed_judgment_dates_do_not_abort_timestamping() {
        assert!(timestamp_from_judgment_date("2024-01-01").is_ok());
        assert!(timestamp_from_judgment_date("20241301").is_ok());
    }

    #[test]
    fn invalid_judgment_date_renders_with_missing_date_fallback() {
        let temp = TempDir::new().unwrap();
        let cache_dir = temp.path().join(".cache").join("precedent");
        fs::create_dir_all(&cache_dir).unwrap();
        write_sample(&cache_dir, "1003", SAMPLE_INVALID_DATE);

        let output = temp.path().join("output.git");
        run(Cli {
            cache_dir,
            output: output.clone(),
            emit_legacy_paths: None,
            legacy_precedent_root: None,
        })
        .unwrap();

        let path = "일반행정/대법원/대법원_0000-00-00_2024구합1.md";
        let markdown = git_stdout(&output, ["show", &format!("HEAD:{path}")]);
        assert!(markdown.contains("판례일련번호: '1003'"));
        assert!(!markdown.contains("선고일자:"));
    }

    #[test]
    fn clamps_pre_epoch_judgment_dates() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("output.git");
        let mut writer = BareRepoWriter::create(&output).unwrap();
        let body = b"body";
        let (blob_sha, compressed_blob) = precompute_blob(body);
        writer
            .commit_precedent(
                &RepoPathBuf::prec_file("민사", "대법원", "1949.md"),
                body,
                blob_sha,
                &compressed_blob,
                "message",
                timestamp_from_judgment_date("19491021").unwrap(),
            )
            .unwrap();
        writer.finish().unwrap();

        assert_eq!(
            git_stdout(&output, ["show", "-s", "--format=%at", "HEAD"]).trim(),
            "10800"
        );
        assert_eq!(
            git_stdout(&output, ["show", "-s", "--format=%ai", "HEAD"]).trim(),
            "1970-01-01 12:00:00 +0900"
        );
    }

    #[test]
    fn empty_judgment_date_uses_unix_epoch() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("output.git");
        let mut writer = BareRepoWriter::create(&output).unwrap();
        let body = b"body";
        let (blob_sha, compressed_blob) = precompute_blob(body);
        writer
            .commit_precedent(
                &RepoPathBuf::prec_file("기타", "미분류", "serial.md"),
                body,
                blob_sha,
                &compressed_blob,
                "message",
                timestamp_from_judgment_date("").unwrap(),
            )
            .unwrap();
        writer.finish().unwrap();

        assert_eq!(
            git_stdout(&output, ["show", "-s", "--format=%at", "HEAD"]).trim(),
            "0"
        );
    }

    fn git_stdout<const N: usize>(repo: &Path, args: [&str; N]) -> String {
        let output = Command::new("git")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }
}
