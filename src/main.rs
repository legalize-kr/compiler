//! Builds a fresh bare Git repository from cached law.go.kr XML and JSON files.
//!
//! The compiler reads an existing `.cache` tree in two passes:
//! metadata is collected and stably sorted first, then each XML document is
//! fully parsed, rendered to Markdown, and committed into a new bare repo.
#![deny(missing_docs)]
#![deny(clippy::missing_docs_in_private_items)]

/// Writes the output bare repository and handcrafted packfile stream.
mod git_repo;
/// Renders parsed law data into Markdown and commit messages.
mod render;
/// Parses cached XML documents into metadata and article structures.
mod xml_parser;

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use mimalloc::MiMalloc;
use rayon::prelude::*;
use rustc_hash::FxHashMap as HashMap;
use serde::Deserialize;

use crate::git_repo::{BareRepoWriter, GitTimestampKst, RepoPathBuf, precompute_blob};
use crate::render::{PathRegistry, build_commit_message, law_to_markdown};
use crate::xml_parser::{LawDetail, LawMetadata, parse_law_body, parse_metadata_only};

/// Bundled README payload for the synthetic initial commit.
const REPOSITORY_README: &[u8] = include_bytes!("../assets/README.md");

/// Global allocator tuned for high-throughput allocation-heavy pack generation.
#[global_allocator]
static GLOBAL_ALLOCATOR: MiMalloc = MiMalloc;

/// Command-line interface for one-shot cache compilation.
#[derive(Debug, Parser)]
#[command(name = "legalize-kr-compiler")]
#[command(about = "Compile cached law.go.kr XML/JSON into a fresh bare Git repository")]
struct Cli {
    /// Path to the existing .cache directory
    cache_dir: PathBuf,

    /// Output bare repository path
    #[arg(short = 'o', long = "output", default_value = "output.git")]
    output: PathBuf,
}

/// Amendment metadata loaded from cached history JSON.
#[derive(Debug, Deserialize)]
struct HistoryEntry {
    /// Law MST key that matches the detail XML filename.
    #[serde(rename = "법령일련번호")]
    mst: String,
    /// Human-readable amendment type applied to that revision.
    #[serde(rename = "제개정구분명", default)]
    amendment: String,
}

/// Pass-1 planning record for one XML document.
#[derive(Debug, Clone)]
struct PlannedEntry {
    /// Law MST used for file lookup and stable ordering.
    mst: String,
    /// Original detail XML path used for targeted warning/error messages.
    source_path: PathBuf,
    /// Final repository path assigned after collision handling.
    path: RepoPathBuf,
    /// Metadata collected during the cheap planning pass.
    metadata: LawMetadata,
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

/*
 * Parsed/rendered chunks stay around 1.4 GiB here, while larger chunks grow memory
 * without materially improving throughput on the real cache workload.
 */
/// Number of entries rendered per worker batch before the writer catches up.
const CHUNK_SIZE: usize = 500;

/// Parses CLI flags and runs the compiler.
fn main() -> Result<()> {
    let cli = Cli::parse();
    run(cli)
}

/// Executes the full two-pass cache-to-Git compilation pipeline.
fn run(cli: Cli) -> Result<()> {
    let cache_dir = cli.cache_dir;
    let detail_dir = cache_dir.join("detail");
    if !detail_dir.is_dir() {
        anyhow::bail!("detail cache not found: {}", detail_dir.display());
    }

    //
    // Load history-side amendment labels before planning commit order.
    //
    eprintln!("1. Loading amendment history");
    // History JSON overrides the amendment labels embedded in detail XML.
    let history = {
        let history_dir = cache_dir.join("history");
        if !history_dir.is_dir() {
            HashMap::default()
        } else {
            let mut files = read_sorted_files(&history_dir, "json")?;
            let mut amendments = HashMap::default();
            for path in files.drain(..) {
                let bytes = fs::read(&path)
                    .with_context(|| format!("failed to read {}", path.display()))?;
                let entries: Vec<HistoryEntry> = serde_json::from_slice(&bytes)
                    .with_context(|| format!("failed to parse {}", path.display()))?;
                for entry in entries {
                    amendments.insert(entry.mst, entry.amendment);
                }
            }
            amendments
        }
    };

    //
    // Pass 1 only touches metadata so every later full parse follows one stable order.
    //
    eprintln!("2. Scanning cache metadata");
    let entries = {
        let files = read_sorted_files(&detail_dir, "xml")?;
        let parsed = files
            .par_iter()
            .map(|path| -> Result<Option<PlannedEntry>> {
                let mst = path
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .map(ToOwned::to_owned)
                    .with_context(|| format!("invalid file name: {}", path.display()))?;
                let xml =
                    fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
                let mut metadata = match parse_metadata_only(&xml, &mst) {
                    Ok(metadata) => metadata,
                    Err(error) => {
                        eprintln!(
                            "warning: skipping unparsable cache file {}: {error:#}",
                            path.display()
                        );
                        return Ok(None);
                    }
                };

                //
                // Some cached detail files are HTML error pages instead of law XML. Those never
                // populate the basic metadata block, so exclude them while keeping every real XML
                // field assumption strict.
                //
                if metadata.law_name.is_empty()
                    && metadata.law_id.is_empty()
                    && metadata.law_type.is_empty()
                    && metadata.promulgation_date.is_empty()
                {
                    eprintln!(
                        "warning: skipping unparsable cache file {}: missing basic law metadata",
                        path.display()
                    );
                    return Ok(None);
                }

                if let Some(amendment) = history.get(&mst) {
                    metadata.amendment = amendment.clone();
                }

                Ok(Some(PlannedEntry {
                    mst,
                    source_path: path.clone(),
                    path: RepoPathBuf::root_file(String::new()),
                    metadata,
                }))
            })
            .collect::<Vec<_>>();

        let mut entries = Vec::with_capacity(files.len());
        for planned in parsed {
            if let Some(planned) = planned? {
                entries.push(planned);
            }
        }

        entries.sort_by(|left, right| {
            left.metadata
                .promulgation_date
                .cmp(&right.metadata.promulgation_date)
                .then_with(|| left.metadata.law_name.cmp(&right.metadata.law_name))
                .then_with(|| {
                    left.metadata
                        .promulgation_number
                        .parse::<u64>()
                        .unwrap_or_else(|error| {
                            panic!(
                                "cache 공포번호 must be numeric: {}: {error:?}",
                                left.source_path.display()
                            )
                        })
                        .cmp(
                            &right
                                .metadata
                                .promulgation_number
                                .parse::<u64>()
                                .unwrap_or_else(|error| {
                                    panic!(
                                        "cache 공포번호 must be numeric: {}: {error:?}",
                                        right.source_path.display()
                                    )
                                }),
                        )
                })
                .then_with(|| {
                    left.mst
                        .parse::<u64>()
                        .expect("detail xml filenames must be numeric MSTs")
                        .cmp(
                            &right
                                .mst
                                .parse::<u64>()
                                .expect("detail xml filenames must be numeric MSTs"),
                        )
                })
        });

        let mut registry = PathRegistry::default();
        for entry in &mut entries {
            entry.path = registry.get_law_path(&entry.metadata.law_name, &entry.metadata.law_type);
        }

        entries
    };
    if entries.is_empty() {
        anyhow::bail!("no valid XML entries found under {}", detail_dir.display());
    }

    //
    // Seed the synthetic history commits that always come before law revisions.
    //
    eprintln!(
        "3. Writing {} commits to {}",
        entries.len(),
        cli.output.display()
    );
    let mut repo = BareRepoWriter::create(&cli.output)?;

    // 2026-03-30 12:00:00 KST (UTC+9) = 2026-03-30 03:00:00 UTC
    const INITIAL_COMMIT_EPOCH: i64 = 1_774_839_600;
    repo.commit_static(
        &RepoPathBuf::root_file("README.md"),
        REPOSITORY_README,
        "initial commit",
        INITIAL_COMMIT_EPOCH,
    )?;
    eprintln!("  committed README.md");
    repo.commit_empty_initial_contributor(
        "Add @simnalamburt as a contributor",
        INITIAL_COMMIT_EPOCH,
    )?;
    eprintln!("  committed contributor marker");

    //
    // Parse/render chunks in parallel while the main thread keeps Git writes ordered.
    //
    let total = entries.len();
    let chunks: Vec<&[PlannedEntry]> = entries.chunks(CHUNK_SIZE).collect();
    let mut pending: Option<Vec<Result<Rendered>>> = None;
    let mut committed = 0usize;

    for (index, chunk) in chunks.iter().enumerate() {
        let detail_dir = detail_dir.to_path_buf();
        let next = if index + 1 < chunks.len() {
            let next_chunk: Vec<PlannedEntry> = chunks[index + 1].to_vec();
            let next_detail_dir = detail_dir.clone();
            Some(std::thread::spawn(move || {
                next_chunk
                    .par_iter()
                    .map(|entry| render_entry(&next_detail_dir, entry))
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
                .map(|entry| render_entry(&detail_dir, entry))
                .collect::<Vec<_>>()
        };

        for rendered in rendered {
            let rendered = rendered?;
            repo.commit_law(
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

    eprintln!("4. Running git index-pack");
    repo.finish()?;
    Ok(())
}

/// Parses, renders, and packages one planned XML entry for pass 2.
fn render_entry(detail_dir: &Path, entry: &PlannedEntry) -> Result<Rendered> {
    // Pass 2 does the expensive XML parse only after pass 1 has fixed the final order and path.
    let xml_path = detail_dir.join(format!("{}.xml", entry.mst));
    let xml =
        fs::read(&xml_path).with_context(|| format!("failed to read {}", xml_path.display()))?;
    let body =
        parse_law_body(&xml).with_context(|| format!("failed to parse {}", xml_path.display()))?;
    let detail = LawDetail {
        metadata: entry.metadata.clone(),
        articles: body.articles,
        addenda: body.addenda,
    };
    let time = GitTimestampKst::from_promulgation_date(&detail.metadata.promulgation_date)?;

    let markdown = law_to_markdown(&detail)?;
    let (blob_sha, compressed_blob) = precompute_blob(&markdown);
    let message = build_commit_message(&detail.metadata, &entry.mst)?;
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::process::Command;

    use tempfile::TempDir;

    use super::*;

    const SAMPLE_XML_1: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<법령>
  <기본정보>
    <법령ID>000001</법령ID>
    <공포일자>20240101</공포일자>
    <공포번호>00001</공포번호>
    <법종구분>법률</법종구분>
    <법령명_한글><![CDATA[테스트법]]></법령명_한글>
    <시행일자>20240101</시행일자>
    <연락부서><부서단위><소관부처명>법무부</소관부처명></부서단위></연락부서>
  </기본정보>
  <조문>
    <조문단위>
      <조문번호>1</조문번호>
      <조문제목><![CDATA[목적]]></조문제목>
      <조문내용><![CDATA[제1조 (목적) 테스트한다.]]></조문내용>
    </조문단위>
  </조문>
</법령>
"#;

    const SAMPLE_INVALID_HTML: &str = r#"<!DOCTYPE html>
<html>
<head><title>Error</title></head>
<body>XML 파싱중 오류 발생</body>
</html>
"#;

    const SAMPLE_XML_2: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<법령>
  <기본정보>
    <법령ID>000002</법령ID>
    <공포일자>20240101</공포일자>
    <공포번호>00002</공포번호>
    <법종구분>대통령령</법종구분>
    <법령명_한글><![CDATA[테스트법 시행령]]></법령명_한글>
    <시행일자>20240101</시행일자>
    <연락부서><부서단위><소관부처명>법무부</소관부처명></부서단위></연락부서>
  </기본정보>
  <조문>
    <조문단위>
      <조문번호>1</조문번호>
      <조문제목><![CDATA[시행]]></조문제목>
      <조문내용><![CDATA[제1조 (시행) 시행한다.]]></조문내용>
    </조문단위>
  </조문>
</법령>
"#;

    const SAMPLE_XML_3: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<법령>
  <기본정보>
    <법령ID>000003</법령ID>
    <공포일자>20240101</공포일자>
    <공포번호>00002</공포번호>
    <법종구분>법률</법종구분>
    <법령명_한글><![CDATA[테스트법]]></법령명_한글>
    <시행일자>20240101</시행일자>
    <연락부서><부서단위><소관부처명>법무부</소관부처명></부서단위></연락부서>
  </기본정보>
  <조문>
    <조문단위>
      <조문번호>2</조문번호>
      <조문제목><![CDATA[개정]]></조문제목>
      <조문내용><![CDATA[제2조 (개정) 테스트를 개정한다.]]></조문내용>
    </조문단위>
  </조문>
</법령>
"#;

    fn write_sample_xml(detail_dir: &Path, mst: &str, xml: &str) {
        fs::write(detail_dir.join(format!("{mst}.xml")), xml).unwrap();
    }

    #[test]
    fn plan_entries_sorts_and_assigns_paths() {
        let temp = TempDir::new().unwrap();
        let detail_dir = temp.path().join("detail");
        fs::create_dir_all(&detail_dir).unwrap();
        write_sample_xml(&detail_dir, "10", SAMPLE_XML_2);
        write_sample_xml(&detail_dir, "1", SAMPLE_XML_1);
        write_sample_xml(&detail_dir, "2", SAMPLE_XML_3);
        write_sample_xml(&detail_dir, "63422", SAMPLE_INVALID_HTML);

        let mut history = HashMap::default();
        history.insert(String::from("1"), String::from("제정"));
        history.insert(String::from("2"), String::from("일부개정"));
        history.insert(String::from("10"), String::from("일부개정"));

        // Keep the unit test aligned with the pass-1 planner logic used by run().
        let entries = {
            let mut files = read_sorted_files(&detail_dir, "xml").unwrap();
            let mut entries = Vec::with_capacity(files.len());

            for path in files.drain(..) {
                let mst = path
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .map(ToOwned::to_owned)
                    .unwrap();
                let xml = fs::read(&path).unwrap();
                let mut metadata = match parse_metadata_only(&xml, &mst) {
                    Ok(metadata) => metadata,
                    Err(_) => continue,
                };

                if metadata.law_name.is_empty()
                    && metadata.law_id.is_empty()
                    && metadata.law_type.is_empty()
                    && metadata.promulgation_date.is_empty()
                {
                    continue;
                }

                if let Some(amendment) = history.get(&mst) {
                    metadata.amendment = amendment.clone();
                }

                entries.push(PlannedEntry {
                    mst,
                    source_path: path.clone(),
                    path: RepoPathBuf::root_file(String::new()),
                    metadata,
                });
            }

            entries.sort_by(|left, right| {
                left.metadata
                    .promulgation_date
                    .cmp(&right.metadata.promulgation_date)
                    .then_with(|| left.metadata.law_name.cmp(&right.metadata.law_name))
                    .then_with(|| {
                        left.metadata
                            .promulgation_number
                            .parse::<u64>()
                            .unwrap_or_else(|error| {
                                panic!(
                                    "cache 공포번호 must be numeric: {}: {error:?}",
                                    left.source_path.display()
                                )
                            })
                            .cmp(
                                &right
                                    .metadata
                                    .promulgation_number
                                    .parse::<u64>()
                                    .unwrap_or_else(|error| {
                                        panic!(
                                            "cache 공포번호 must be numeric: {}: {error:?}",
                                            right.source_path.display()
                                        )
                                    }),
                            )
                    })
                    .then_with(|| {
                        left.mst
                            .parse::<u64>()
                            .expect("detail xml filenames must be numeric MSTs")
                            .cmp(
                                &right
                                    .mst
                                    .parse::<u64>()
                                    .expect("detail xml filenames must be numeric MSTs"),
                            )
                    })
            });

            let mut registry = PathRegistry::default();
            for entry in &mut entries {
                entry.path =
                    registry.get_law_path(&entry.metadata.law_name, &entry.metadata.law_type);
            }

            entries
        };
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].mst, "1");
        assert_eq!(entries[0].path, RepoPathBuf::kr_file("테스트법", "법률.md"));
        assert_eq!(entries[1].mst, "2");
        assert_eq!(entries[1].path, RepoPathBuf::kr_file("테스트법", "법률.md"));
        assert_eq!(entries[2].mst, "10");
        assert_eq!(
            entries[2].path,
            RepoPathBuf::kr_file("테스트법", "시행령.md")
        );
    }

    #[test]
    fn end_to_end_builds_bare_repo() {
        let temp = TempDir::new().unwrap();
        let cache_dir = temp.path().join(".cache");
        let detail_dir = cache_dir.join("detail");
        let history_dir = cache_dir.join("history");
        fs::create_dir_all(&detail_dir).unwrap();
        fs::create_dir_all(&history_dir).unwrap();
        write_sample_xml(&detail_dir, "1", SAMPLE_XML_1);
        write_sample_xml(&detail_dir, "2", SAMPLE_XML_2);
        fs::write(
            history_dir.join("테스트법.json"),
            r#"[{"법령일련번호":"1","제개정구분명":"제정"},{"법령일련번호":"2","제개정구분명":"일부개정"}]"#,
        )
        .unwrap();

        let output = temp.path().join("output.git");
        run(Cli {
            cache_dir,
            output: output.clone(),
        })
        .unwrap();

        assert_eq!(
            git_stdout(&output, ["symbolic-ref", "--short", "HEAD"]).trim(),
            "main"
        );
        assert_eq!(
            git_stdout(&output, ["rev-list", "--count", "HEAD"]).trim(),
            "4"
        );

        let commits = git_stdout(&output, ["rev-list", "--reverse", "HEAD"]);
        let commits = commits.lines().collect::<Vec<_>>();
        assert_eq!(commits.len(), 4);

        let contributor_author = git_stdout(
            &output,
            ["show", "-s", "--format=%an%n%ae%n%cn%n%ce", commits[1]],
        );
        assert_eq!(
            contributor_author.lines().collect::<Vec<_>>(),
            vec![
                "Jihyeon Kim",
                "simnalamburt@gmail.com",
                "Jihyeon Kim",
                "simnalamburt@gmail.com"
            ]
        );

        let contributor_tree = git_stdout(&output, ["show", "-s", "--format=%T", commits[1]]);
        let readme_tree = git_stdout(&output, ["show", "-s", "--format=%T", commits[0]]);
        assert_eq!(contributor_tree.trim(), readme_tree.trim());

        let contributor_time = git_stdout(&output, ["show", "-s", "--format=%at %ai", commits[1]]);
        let readme_time = git_stdout(&output, ["show", "-s", "--format=%at %ai", commits[0]]);
        assert_eq!(contributor_time.trim(), readme_time.trim());
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
