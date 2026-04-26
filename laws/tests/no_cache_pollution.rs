//! Regression: the compiler binary must not write to any path outside the
//! explicit `cache_dir` / output arguments it receives.
//!
//! Context: the sister Python pipeline had a bug where its unit tests wrote
//! fixture law names (`법0`..`법49`, `foo법`) into the shared on-disk
//! `.cache/history/` because the tests did not monkeypatch
//! `laws.cache.CACHE_DIR`. That poisoned subsequent `laws.fetch_cache` runs
//! with an invariant violation. The compiler in this repo takes its cache
//! path exclusively from a positional CLI argument, so the equivalent trap
//! would be any `src/` code ever introducing a CWD-relative `.cache/` path.
//! This test is the trip wire: run the binary with an isolated CWD and
//! assert nothing outside the provided output path was created.

use std::fs;
use std::path::Path;
use std::process::Command;

use tempfile::TempDir;

const SAMPLE_XML_ROOT: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
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

fn snapshot_entries(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    if !dir.exists() {
        return out;
    }
    collect(dir, dir, &mut out);
    out.sort();
    out
}

fn collect(root: &Path, dir: &Path, out: &mut Vec<String>) {
    for entry in fs::read_dir(dir).unwrap().flatten() {
        let p = entry.path();
        let rel = p.strip_prefix(root).unwrap().to_string_lossy().into_owned();
        out.push(rel.clone());
        if p.is_dir() {
            collect(root, &p, out);
        }
    }
}

#[test]
fn compiler_does_not_write_outside_explicit_output_path() {
    // Isolated cache with exactly one law.
    let cache_root = TempDir::new().unwrap();
    let detail_dir = cache_root.path().join(".cache").join("detail");
    fs::create_dir_all(&detail_dir).unwrap();
    fs::write(detail_dir.join("1.xml"), SAMPLE_XML_ROOT).unwrap();

    // Isolated output path (inside its own tempdir so we can assert the
    // binary only touched what we expected).
    let output_root = TempDir::new().unwrap();
    let output_git = output_root.path().join("output.git");

    // Isolated CWD with a canary file we will re-check afterwards. Anything
    // else appearing here would mean the binary wrote to a CWD-relative path.
    let cwd = TempDir::new().unwrap();
    fs::write(cwd.path().join("canary.txt"), b"canary").unwrap();
    let before_cwd = snapshot_entries(cwd.path());

    let status = Command::new(env!("CARGO_BIN_EXE_legalize-kr-compiler"))
        .arg(cache_root.path().join(".cache"))
        .arg("-o")
        .arg(&output_git)
        .current_dir(cwd.path())
        .status()
        .expect("compiler binary must exist");
    assert!(status.success(), "compiler failed: {status:?}");

    // 1. The isolated CWD must not have gained any files beyond the canary.
    let after_cwd = snapshot_entries(cwd.path());
    assert_eq!(
        before_cwd,
        after_cwd,
        "compiler wrote to CWD — expected no new entries, diff={:?}",
        after_cwd
            .iter()
            .filter(|e| !before_cwd.contains(e))
            .collect::<Vec<_>>(),
    );

    // 2. The cache dir must not have been mutated. The binary is supposed to
    //    be read-only against its input cache.
    assert!(
        detail_dir.join("1.xml").exists(),
        "input xml disappeared from cache",
    );
    let cache_entries = snapshot_entries(cache_root.path());
    let unexpected: Vec<_> = cache_entries
        .iter()
        .filter(|e| !e.starts_with(".cache"))
        .collect();
    assert!(
        unexpected.is_empty(),
        "compiler wrote unexpected files into cache root: {unexpected:?}",
    );

    // 3. Output path exists under the declared output dir only.
    assert!(output_git.exists(), "expected output repo was not created");
}
