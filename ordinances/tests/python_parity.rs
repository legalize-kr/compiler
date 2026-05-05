use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const SAMPLE_XML: &str = r#"<자치법규>
  <자치법규ID>2000111</자치법규ID>
  <자치법규일련번호>12345</자치법규일련번호>
  <자치법규명>서울특별시 테스트 조례</자치법규명>
  <자치법규종류>C0001</자치법규종류>
  <지자체기관명>서울특별시</지자체기관명>
  <공포일자>20210930</공포일자>
  <공포번호>7825</공포번호>
  <시행일자>20210930</시행일자>
  <제개정구분명>일부개정</제개정구분명>
  <자치법규분야명>일반공공행정</자치법규분야명>
  <담당부서명>법무담당관</담당부서명>
  <조문단위>
    <조문번호>1</조문번호>
    <조문제목>목적</조문제목>
    <조문내용>제1조(목적) 이 조례는 테스트를 목적으로 한다.</조문내용>
  </조문단위>
  <별표>
    <별표단위 별표키="1">
      <별표번호>0001</별표번호>
      <별표가지번호>00</별표가지번호>
      <별표구분>서식</별표구분>
      <별표제목><![CDATA[[별지 제1호서식] 신청서]]></별표제목>
      <별표첨부파일구분>hwp</별표첨부파일구분>
      <별표첨부파일명><![CDATA[http://www.law.go.kr/flDownload.do?gubun=ELIS&flSeq=1&flNm=test]]></별표첨부파일명>
    </별표단위>
  </별표>
</자치법규>"#;

#[test]
fn fixture_matches_python_pipeline_converter() {
    let Some(pipeline) = pipeline_dir() else {
        eprintln!("skipping Python parity test: legalize-pipeline checkout not found");
        return;
    };
    assert!(
        pipeline.join("ordinances").is_dir(),
        "legalize-pipeline checkout does not contain ordinances/: {}",
        pipeline.display()
    );

    let temp = tempfile::tempdir().unwrap();
    let cache_dir = temp.path().join("cache");
    let output_dir = temp.path().join("out");
    fs::create_dir(&cache_dir).unwrap();
    fs::write(cache_dir.join("2000111.xml"), SAMPLE_XML).unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_ordinance-kr-compiler"))
        .arg(&cache_dir)
        .arg("-o")
        .arg(&output_dir)
        .arg("--tree")
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
    let pipeline = pipeline_dir().expect("legalize-pipeline checkout is required");
    let script = r#"
import sys
from ordinances import converter
xml = sys.stdin.read()
path, markdown = converter.xml_to_markdown(xml)
print(path)
print("===MARKDOWN===")
print(markdown, end="")
"#;
    let output = Command::new(std::env::var("PYTHON").unwrap_or_else(|_| "python".to_string()))
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

fn pipeline_dir() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("LEGALIZE_PIPELINE_ROOT") {
        return Some(PathBuf::from(path));
    }
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let mut candidates = vec![repo_root.join("legalize-pipeline")];
    if let Some(parent) = repo_root.parent() {
        candidates.push(parent.join("legalize-pipeline"));
    }
    candidates
        .into_iter()
        .find(|path| path.join("ordinances").is_dir())
}
