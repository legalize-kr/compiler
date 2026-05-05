//! Byte-stable end-to-end snapshot for the laws compiler binary.
//!
//! Drives `legalize-kr-compiler` against a tiny inline XML fixture and asserts
//! the produced Markdown file is byte-identical to a known snapshot. This is the
//! laws-side counterpart to `precedents/tests/oracle.rs` (which only covers
//! filename composition); it covers the full Markdown body — frontmatter,
//! Unicode normalization, article rendering, and trailing whitespace — so any
//! drift in render logic surfaces here, not in a downstream rebuild diff.
//!
//! This test was the regression checkpoint for the workspace merge that folded
//! `compiler-for-precedent` into this repository on 2026-04-26: pre-merge and
//! post-merge binaries produce the same hash for the same fixture.

use std::fs;
use std::path::Path;
use std::process::Command;

use tempfile::TempDir;

const SAMPLE_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
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

const EXPECTED_PATH: &str = "kr/테스트법/법률.md";
const EXPECTED_CONTENT: &str = "---
제목: 테스트법
법령MST: 1
법령ID: '000001'
법령구분: 법률
법령구분코드: ''
소관부처:
- 법무부
공포일자: 2024-01-01
공포번호: '00001'
시행일자: 2024-01-01
법령분야: ''
상태: 시행
출처: https://www.law.go.kr/법령/테스트법
첨부파일: []
---

# 테스트법

##### 제1조 (목적)

테스트한다.

";

fn write_fixture(detail_dir: &Path) {
    fs::write(detail_dir.join("000001.xml"), SAMPLE_XML).unwrap();
}

#[test]
fn law_markdown_is_byte_stable() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path().join(".cache");
    let detail_dir = cache_dir.join("detail");
    fs::create_dir_all(&detail_dir).unwrap();
    write_fixture(&detail_dir);

    let bare = temp.path().join("output.git");
    let status = Command::new(env!("CARGO_BIN_EXE_legalize-kr-compiler"))
        .arg(&cache_dir)
        .arg("-o")
        .arg(&bare)
        .status()
        .unwrap();
    assert!(status.success(), "compiler exited with {:?}", status.code());

    // Clone the bare repo into a working tree so we can read files by path.
    let work = temp.path().join("checkout");
    let clone = Command::new("git")
        .args(["clone", "--quiet"])
        .arg(&bare)
        .arg(&work)
        .status()
        .unwrap();
    assert!(clone.success(), "git clone failed");

    let actual_path = work.join(EXPECTED_PATH);
    assert!(
        actual_path.exists(),
        "expected output file not present: {}",
        actual_path.display()
    );
    let actual = fs::read_to_string(&actual_path).unwrap();
    assert_eq!(
        actual, EXPECTED_CONTENT,
        "Markdown output drift detected at {EXPECTED_PATH}.\n\
         If this change was intentional, regenerate the snapshot with:\n  \
         legalize-kr-compiler <cache-with-fixture> -o /tmp/snap.git && \\\n  \
         git -C /tmp/snap.git show HEAD:{EXPECTED_PATH}"
    );
}
