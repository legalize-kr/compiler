use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const SAMPLE_XML: &str = r#"<AdmRulService>
  <행정규칙ID>ABC</행정규칙ID>
  <행정규칙일련번호>123</행정규칙일련번호>
  <행정규칙명>공공데이터 관리지침</행정규칙명>
  <행정규칙종류>고시</행정규칙종류>
  <소관부처명>행정안전부</소관부처명>
  <기관코드>1741000</기관코드>
  <발령번호>제2024-1호</발령번호>
  <발령일자>20240504</발령일자>
  <시행일자>20240505</시행일자>
  <제개정구분명>일부개정</제개정구분명>
  <제개정구분코드>200402</제개정구분코드>
  <현행연혁구분>현행</현행연혁구분>
  <조문내용>제1조 목적</조문내용>
</AdmRulService>"#;

#[test]
fn fixture_matches_python_pipeline_converter() {
    if !pipeline_dir().join("admrules").is_dir() {
        eprintln!("skipping Python parity test: legalize-pipeline checkout not found");
        return;
    }

    let temp = tempfile::tempdir().unwrap();
    let cache_dir = temp.path().join("cache");
    let output_dir = temp.path().join("out");
    fs::create_dir(&cache_dir).unwrap();
    fs::write(cache_dir.join("123.xml"), SAMPLE_XML).unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_admrule-kr-compiler"))
        .arg(&cache_dir)
        .arg("-o")
        .arg(&output_dir)
        .status()
        .unwrap();
    assert!(status.success());

    let (expected_path, expected_markdown) = python_reference(SAMPLE_XML);
    let actual_path = output_dir.join(Path::new(&expected_path));
    let actual_markdown = fs::read_to_string(&actual_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", actual_path.display()));

    assert_eq!(actual_markdown, expected_markdown);
}

fn python_reference(xml: &str) -> (String, String) {
    let pipeline = pipeline_dir();
    let script = r#"
import sys
from xml.etree import ElementTree
from admrules import converter
xml = sys.stdin.read()
root = ElementTree.fromstring(xml)
metadata = converter._metadata_from_xml(root)
converter.reset_path_registry()
print(converter.get_admrule_path(metadata))
print("===MARKDOWN===")
print(converter.xml_to_markdown(xml), end="")
"#;
    let output = Command::new("python")
        .arg("-c")
        .arg(script)
        .env("PYTHONPATH", &pipeline)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child.stdin.as_mut().unwrap().write_all(xml.as_bytes())?;
            child.wait_with_output()
        })
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let (path, markdown) = stdout.split_once("\n===MARKDOWN===\n").unwrap();
    (path.to_string(), markdown.to_string())
}

fn pipeline_dir() -> PathBuf {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    repo_root.join("legalize-pipeline")
}
