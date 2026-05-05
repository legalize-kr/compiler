//! Byte-stable end-to-end snapshot for the precedents compiler binary.
//!
//! Complements `oracle.rs` (which only covers filename composition) by asserting
//! the full Markdown body — frontmatter ordering, court-tier mapping, source URL,
//! body section headers, trailing whitespace — is byte-identical to a snapshot.
//!
//! This was the regression checkpoint for the 2026-04-26 workspace merge that
//! folded this crate into the `compiler` repository: pre-merge and post-merge
//! binaries hash identically against the same fixture.

use std::fs;
use std::path::Path;
use std::process::Command;

use tempfile::TempDir;

const SAMPLE_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<PrecService>
  <판례정보일련번호>100001</판례정보일련번호>
  <사건명><![CDATA[테스트 판결]]></사건명>
  <사건번호>2020다12345</사건번호>
  <선고일자>20200310</선고일자>
  <법원명>대법원</법원명>
  <법원종류코드>400201</법원종류코드>
  <사건종류명>민사</사건종류명>
  <사건종류코드>400101</사건종류코드>
  <판결유형>판결</판결유형>
  <선고>선고</선고>
  <판시사항><![CDATA[판시사항 본문]]></판시사항>
  <판결요지><![CDATA[판결요지 본문]]></판결요지>
  <판례내용><![CDATA[판례내용 본문]]></판례내용>
</PrecService>
"#;

const EXPECTED_PATH: &str = "민사/대법원/대법원_2020-03-10_2020다12345.md";
const EXPECTED_CONTENT: &str = "---
판례일련번호: '100001'
사건번호: 2020다12345
사건명: 테스트 판결
법원명: 대법원
법원등급: 대법원
사건종류: 민사
출처: https://www.law.go.kr/LSW/precInfoP.do?precSeq=100001
첨부파일: []
선고일자: 2020-03-10
---

# 테스트 판결

## 판시사항

판시사항 본문

## 판결요지

판결요지 본문

## 판례내용

판례내용 본문

";

fn write_fixture(cache_dir: &Path) {
    fs::write(cache_dir.join("100001.xml"), SAMPLE_XML).unwrap();
}

#[test]
fn precedent_markdown_is_byte_stable() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path().join("precedent");
    fs::create_dir_all(&cache_dir).unwrap();
    write_fixture(&cache_dir);

    let bare = temp.path().join("output.git");
    let status = Command::new(env!("CARGO_BIN_EXE_precedent-kr-compiler"))
        .arg(&cache_dir)
        .arg("-o")
        .arg(&bare)
        .status()
        .unwrap();
    assert!(status.success(), "compiler exited with {:?}", status.code());

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
         precedent-kr-compiler <cache-with-fixture> -o /tmp/snap.git && \\\n  \
         git -C /tmp/snap.git show 'HEAD:{EXPECTED_PATH}'"
    );
}
