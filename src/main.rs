mod git_repo;
mod pack_repo;
mod render;
mod xml_parser;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;

use crate::git_repo::BareRepoWriter;
use crate::pack_repo::PackRepoWriter;
use crate::render::{PathRegistry, build_commit_message, law_to_markdown};
use crate::xml_parser::{LawMetadata, parse_law_detail, parse_metadata_only};

#[derive(Debug, Parser)]
#[command(name = "legalize-kr-compiler")]
#[command(about = "Compile cached law.go.kr XML/JSON into a fresh bare Git repository")]
struct Cli {
    /// Path to the existing .cache directory
    cache_dir: PathBuf,

    /// Output bare repository path
    #[arg(short = 'o', long = "output", default_value = "output.git")]
    output: PathBuf,

    /// Path to README.md to include in the repository
    #[arg(long = "readme")]
    readme: Option<PathBuf>,

    /// Use alternative packfile writer instead of libgit2
    #[arg(long = "alternative")]
    alternative: bool,
}

#[derive(Debug, Deserialize)]
struct HistoryEntry {
    #[serde(rename = "법령일련번호")]
    mst: String,
    #[serde(rename = "제개정구분명", default)]
    amendment: String,
}

#[derive(Debug, Clone)]
struct PlannedEntry {
    mst: String,
    path: String,
    metadata: LawMetadata,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    run(cli)
}

fn run(cli: Cli) -> Result<()> {
    let cache_dir = cli.cache_dir;
    let detail_dir = cache_dir.join("detail");
    if !detail_dir.is_dir() {
        anyhow::bail!("detail cache not found: {}", detail_dir.display());
    }

    eprintln!("loading amendment history...");
    let history = load_amendments(&cache_dir)?;

    eprintln!("pass 1/2: scanning cache metadata...");
    let entries = plan_entries(&detail_dir, &history)?;
    if entries.is_empty() {
        anyhow::bail!("no valid XML entries found under {}", detail_dir.display());
    }

    eprintln!(
        "writing {} commits to {}...",
        entries.len(),
        cli.output.display()
    );

    if cli.alternative {
        run_alternative(&cli.output, &cli.readme, &detail_dir, &entries)?;
    } else {
        run_default(&cli.output, &cli.readme, &detail_dir, &entries)?;
    }

    eprintln!("done");
    Ok(())
}

fn run_default(
    output: &Path,
    readme: &Option<PathBuf>,
    detail_dir: &Path,
    entries: &[PlannedEntry],
) -> Result<()> {
    let mut repo = BareRepoWriter::create(output)?;

    if let Some(readme_path) = readme {
        let readme = fs::read(readme_path)
            .with_context(|| format!("failed to read {}", readme_path.display()))?;
        repo.commit_static("README.md", &readme, "initial commit", 1_774_839_600, 540)?;
        eprintln!("  committed README.md");
    }

    for (index, entry) in entries.iter().enumerate() {
        let xml_path = detail_dir.join(format!("{}.xml", entry.mst));
        let xml = fs::read(&xml_path)
            .with_context(|| format!("failed to read {}", xml_path.display()))?;
        let mut detail = parse_law_detail(&xml, &entry.mst)
            .with_context(|| format!("failed to parse {}", xml_path.display()))?;
        detail.metadata.amendment = entry.metadata.amendment.clone();

        let markdown = law_to_markdown(&detail)?;
        let commit_message = build_commit_message(&detail.metadata, &entry.mst);
        repo.commit_law(
            &entry.path,
            &markdown,
            &commit_message,
            &detail.metadata.promulgation_date,
        )
        .with_context(|| format!("failed to commit MST {}", entry.mst))?;

        if (index + 1) % 500 == 0 || index + 1 == entries.len() {
            eprintln!("  committed {}/{}", index + 1, entries.len());
        }
    }

    repo.finish()?;
    Ok(())
}

struct Rendered {
    path: String,
    markdown: Vec<u8>,
    message: String,
    promulgation_date: String,
}

fn render_entry(detail_dir: &Path, entry: &PlannedEntry) -> Result<Rendered> {
    let xml_path = detail_dir.join(format!("{}.xml", entry.mst));
    let xml = fs::read(&xml_path)
        .with_context(|| format!("failed to read {}", xml_path.display()))?;
    let mut detail = parse_law_detail(&xml, &entry.mst)
        .with_context(|| format!("failed to parse MST {}", entry.mst))?;
    detail.metadata.amendment = entry.metadata.amendment.clone();
    let markdown = law_to_markdown(&detail)?;
    let message = build_commit_message(&detail.metadata, &entry.mst);
    Ok(Rendered {
        path: entry.path.clone(),
        markdown,
        message,
        promulgation_date: detail.metadata.promulgation_date,
    })
}

const CHUNK_SIZE: usize = 1000;

fn run_alternative(
    output: &Path,
    readme: &Option<PathBuf>,
    detail_dir: &Path,
    entries: &[PlannedEntry],
) -> Result<()> {
    use rayon::prelude::*;

    let mut repo = PackRepoWriter::create(output)?;

    if let Some(readme_path) = readme {
        let readme = fs::read(readme_path)
            .with_context(|| format!("failed to read {}", readme_path.display()))?;
        repo.commit_static("README.md", &readme, "initial commit", 1_774_839_600, 540)?;
        eprintln!("  committed README.md");
    }

    let total = entries.len();
    let mut committed = 0;
    let mut skipped = 0usize;

    let chunks: Vec<&[PlannedEntry]> = entries.chunks(CHUNK_SIZE).collect();
    let mut pending: Option<Vec<Result<Rendered>>> = None;

    for (ci, chunk) in chunks.iter().enumerate() {
        let detail_dir = detail_dir.to_path_buf();
        /* Start parsing next chunk in background while committing current */
        let next = if ci + 1 < chunks.len() {
            let next_chunk: Vec<PlannedEntry> = chunks[ci + 1].to_vec();
            let dd = detail_dir.clone();
            Some(std::thread::spawn(move || -> Vec<Result<Rendered>> {
                next_chunk.par_iter().map(|entry| render_entry(&dd, entry)).collect()
            }))
        } else {
            None
        };

        /* Commit current chunk (first iteration parses synchronously) */
        let rendered = if let Some(prev) = pending.take() {
            prev
        } else {
            chunk.par_iter().map(|entry| render_entry(&detail_dir, entry)).collect()
        };

        for r in rendered {
            match r {
                Ok(r) => {
                    repo.commit_law(&r.path, &r.markdown, &r.message, &r.promulgation_date)?;
                    committed += 1;
                    if committed % 500 == 0 || committed == total {
                        eprintln!("  committed {committed}/{total}");
                    }
                }
                Err(e) => {
                    skipped += 1;
                    eprintln!("  WARN: {e:#}");
                }
            }
        }

        /* Collect next chunk's results */
        if let Some(handle) = next {
            pending = Some(handle.join().unwrap());
        }
    }

    if skipped > 0 {
        eprintln!("  skipped {skipped} entries due to errors");
    }

    repo.finish()?;
    Ok(())
}

fn load_amendments(cache_dir: &Path) -> Result<HashMap<String, String>> {
    let history_dir = cache_dir.join("history");
    if !history_dir.is_dir() {
        return Ok(HashMap::new());
    }

    let mut files = read_sorted_files(&history_dir, "json")?;
    let mut amendments = HashMap::new();
    for path in files.drain(..) {
        let bytes =
            fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let entries: Vec<HistoryEntry> = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        for entry in entries {
            if !entry.mst.is_empty() {
                amendments.insert(entry.mst, entry.amendment);
            }
        }
    }
    Ok(amendments)
}

fn plan_entries(
    detail_dir: &Path,
    amendments: &HashMap<String, String>,
) -> Result<Vec<PlannedEntry>> {
    let mut files = read_sorted_files(detail_dir, "xml")?;
    let mut entries = Vec::with_capacity(files.len());
    let mut skipped_blank_name = 0usize;

    for path in files.drain(..) {
        let mst = path
            .file_stem()
            .and_then(|name| name.to_str())
            .map(ToOwned::to_owned)
            .with_context(|| format!("invalid file name: {}", path.display()))?;
        let xml = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let mut metadata = parse_metadata_only(&xml, &mst)
            .with_context(|| format!("failed to parse {}", path.display()))?;

        if let Some(amendment) = amendments.get(&mst) {
            metadata.amendment = amendment.clone();
        }

        if metadata.law_name.trim().is_empty() {
            skipped_blank_name += 1;
            continue;
        }

        entries.push(PlannedEntry {
            mst,
            path: String::new(),
            metadata,
        });
    }

    entries.sort_by(|left, right| {
        left.metadata
            .promulgation_date
            .cmp(&right.metadata.promulgation_date)
            .then_with(|| left.metadata.law_name.cmp(&right.metadata.law_name))
            .then_with(|| {
                compare_optional_numeric(
                    &left.metadata.promulgation_number,
                    &right.metadata.promulgation_number,
                )
            })
            .then_with(|| compare_numeric(&left.mst, &right.mst))
    });

    let mut registry = PathRegistry::default();
    for entry in &mut entries {
        entry.path = registry.get_law_path(&entry.metadata.law_name, &entry.metadata.law_type);
    }

    if skipped_blank_name > 0 {
        eprintln!(
            "  skipped {} entries with empty law names",
            skipped_blank_name
        );
    }

    Ok(entries)
}

fn compare_optional_numeric(left: &str, right: &str) -> Ordering {
    match (parse_numeric_key(left), parse_numeric_key(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => left.cmp(right),
    }
}

fn compare_numeric(left: &str, right: &str) -> Ordering {
    match (parse_numeric_key(left), parse_numeric_key(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

fn parse_numeric_key(value: &str) -> Option<u64> {
    value.parse().ok()
}

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

    use git2::Repository;
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

    #[test]
    fn plan_entries_sorts_and_assigns_paths() {
        let temp = TempDir::new().unwrap();
        let detail_dir = temp.path().join("detail");
        fs::create_dir_all(&detail_dir).unwrap();
        fs::write(detail_dir.join("10.xml"), SAMPLE_XML_2).unwrap();
        fs::write(detail_dir.join("1.xml"), SAMPLE_XML_1).unwrap();
        fs::write(detail_dir.join("2.xml"), SAMPLE_XML_3).unwrap();

        let mut history = HashMap::new();
        history.insert(String::from("1"), String::from("제정"));
        history.insert(String::from("2"), String::from("일부개정"));
        history.insert(String::from("10"), String::from("일부개정"));

        let entries = plan_entries(&detail_dir, &history).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].mst, "1");
        assert_eq!(entries[0].path, "kr/테스트법/법률.md");
        assert_eq!(entries[1].mst, "2");
        assert_eq!(entries[1].path, "kr/테스트법/법률.md");
        assert_eq!(entries[2].mst, "10");
        assert_eq!(entries[2].path, "kr/테스트법/시행령.md");
    }

    #[test]
    fn end_to_end_builds_bare_repo() {
        let temp = TempDir::new().unwrap();
        let cache_dir = temp.path().join(".cache");
        let detail_dir = cache_dir.join("detail");
        let history_dir = cache_dir.join("history");
        fs::create_dir_all(&detail_dir).unwrap();
        fs::create_dir_all(&history_dir).unwrap();
        fs::write(detail_dir.join("1.xml"), SAMPLE_XML_1).unwrap();
        fs::write(detail_dir.join("2.xml"), SAMPLE_XML_2).unwrap();
        fs::write(
            history_dir.join("테스트법.json"),
            r#"[{"법령일련번호":"1","제개정구분명":"제정"},{"법령일련번호":"2","제개정구분명":"일부개정"}]"#,
        )
        .unwrap();

        let output = temp.path().join("output.git");
        run(Cli {
            cache_dir,
            output: output.clone(),
            readme: None,
            alternative: false,
        })
        .unwrap();

        let repo = Repository::open_bare(output).unwrap();
        let head = repo.head().unwrap();
        assert_eq!(head.shorthand(), Some("main"));
        let mut revwalk = repo.revwalk().unwrap();
        revwalk.push_head().unwrap();
        assert_eq!(revwalk.count(), 2);
    }

    #[test]
    fn end_to_end_alternative_builds_bare_repo() {
        let temp = TempDir::new().unwrap();
        let cache_dir = temp.path().join(".cache");
        let detail_dir = cache_dir.join("detail");
        let history_dir = cache_dir.join("history");
        fs::create_dir_all(&detail_dir).unwrap();
        fs::create_dir_all(&history_dir).unwrap();
        fs::write(detail_dir.join("1.xml"), SAMPLE_XML_1).unwrap();
        fs::write(detail_dir.join("2.xml"), SAMPLE_XML_2).unwrap();
        fs::write(detail_dir.join("3.xml"), SAMPLE_XML_3).unwrap();
        fs::write(
            history_dir.join("테스트법.json"),
            r#"[{"법령일련번호":"1","제개정구분명":"제정"},{"법령일련번호":"2","제개정구분명":"일부개정"}]"#,
        )
        .unwrap();
        fs::write(
            history_dir.join("테스트법 시행령.json"),
            r#"[{"법령일련번호":"10","제개정구분명":"일부개정"}]"#,
        )
        .unwrap();

        let output = temp.path().join("output.git");
        run(Cli {
            cache_dir,
            output: output.clone(),
            readme: None,
            alternative: true,
        })
        .unwrap();

        /* verify with git CLI */
        let status = std::process::Command::new("git")
            .args(["--git-dir", output.to_str().unwrap(), "rev-list", "--count", "HEAD"])
            .output()
            .unwrap();
        let count: usize = String::from_utf8_lossy(&status.stdout).trim().parse().unwrap();
        assert_eq!(count, 3);

        let status = std::process::Command::new("git")
            .args(["--git-dir", output.to_str().unwrap(), "fsck", "--no-progress"])
            .output()
            .unwrap();
        assert!(status.status.success(), "git fsck failed: {}", String::from_utf8_lossy(&status.stderr));
    }
}
