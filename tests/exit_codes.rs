//! Integration tests that drive the compiler binary as a subprocess to observe exit codes.

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

const SAMPLE_XML_CHILD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
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

fn write_sample_xml(detail_dir: &Path, mst: &str, xml: &str) {
    fs::write(detail_dir.join(format!("{mst}.xml")), xml).unwrap();
}

#[test]
fn on_anomaly_fail_exits_with_code_2() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path().join(".cache");
    let detail_dir = cache_dir.join("detail");
    fs::create_dir_all(&detail_dir).unwrap();
    // Orphan 시행령 without its parent 법률 triggers an anomaly under --strict.
    write_sample_xml(&detail_dir, "10", SAMPLE_XML_CHILD);

    let output = temp.path().join("output.git");
    let status = Command::new(env!("CARGO_BIN_EXE_legalize-kr-compiler"))
        .arg(&cache_dir)
        .arg("-o")
        .arg(&output)
        .arg("--strict")
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(2));
}

#[test]
fn expect_laws_mismatch_exits_with_code_3() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path().join(".cache");
    let detail_dir = cache_dir.join("detail");
    fs::create_dir_all(&detail_dir).unwrap();
    write_sample_xml(&detail_dir, "1", SAMPLE_XML_ROOT);

    let output = temp.path().join("output.git");
    let status = Command::new(env!("CARGO_BIN_EXE_legalize-kr-compiler"))
        .arg(&cache_dir)
        .arg("-o")
        .arg(&output)
        .arg("--expect-laws")
        .arg("999")
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(3));
}
