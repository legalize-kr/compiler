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
  <별표>
    <별표단위>
      <별표번호>0001</별표번호>
      <별표가지번호>00</별표가지번호>
      <별표구분>별표</별표구분>
      <별표제목><![CDATA[수수료]]></별표제목>
      <별표서식파일링크>/LSW/flDownload.do?flSeq=1</별표서식파일링크>
      <별표서식PDF파일링크>/LSW/flDownload.do?flSeq=2</별표서식PDF파일링크>
    </별표단위>
    <별표단위>
      <별표번호>0001</별표번호>
      <별표가지번호>01</별표가지번호>
      <별표구분>별지</별표구분>
      <별표제목><![CDATA[신청서]]></별표제목>
      <별표서식파일링크>/LSW/flDownload.do?flSeq=3</별표서식파일링크>
    </별표단위>
  </별표>
</AdmRulService>"#;

#[test]
fn fixture_matches_python_pipeline_converter() {
    let Some(pipeline) = pipeline_dir() else {
        eprintln!("skipping Python parity test: legalize-pipeline checkout not found");
        return;
    };
    assert!(
        pipeline.join("admrules").is_dir(),
        "legalize-pipeline checkout does not contain admrules/: {}",
        pipeline.display()
    );

    let temp = tempfile::tempdir().unwrap();
    let cache_dir = temp.path().join("cache");
    let output_dir = temp.path().join("out");
    fs::create_dir(&cache_dir).unwrap();
    fs::write(cache_dir.join("123.xml"), SAMPLE_XML).unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_admrule-kr-compiler"))
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

    assert!(actual_markdown.contains("본문출처: 'api-text'"));
    assert_eq!(actual_markdown.matches("\n- 별표번호:").count(), 2);
    for legacy_key in [
        "source_url:",
        "body_source:",
        "hwp_sha256:",
        "attachments_hwp:",
        "epoch_clamped:",
        "발령일자_raw:",
    ] {
        assert!(!actual_markdown.contains(legacy_key));
    }

    assert_eq!(actual_markdown, expected_markdown);
}

fn python_reference(xml: &str) -> (String, String) {
    let pipeline = pipeline_dir().expect("legalize-pipeline checkout is required");
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
        .find(|path| path.join("admrules").is_dir())
}

#[test]
fn current_snapshot_preserves_future_history_and_matches_python_import() {
    let pipeline = pipeline_dir().expect("Python pipeline required for snapshot parity");
    let temp = tempfile::tempdir().unwrap();
    let cache = temp.path().join("cache");
    fs::create_dir(&cache).unwrap();
    let old = SAMPLE_XML
        .replace("행정안전부", "문화재청")
        .replace("20240505", "199919")
        .replace("제1조 목적", "제1조 목적\n３. 전각 숫자 항목")
        .replace(
            "수수료",
            &format!(
                "{}\n (줄바꿈 및 \"인용\")\t별표",
                "領事機關 긴 첨부파일 제목 ".repeat(20)
            ),
        )
        .replace("고시", "세칙")
        .replace("일부개정", "폐지제정")
        .replace("200402", "200407");
    let future = old
        .replace("123", "124")
        .replace("공공데이터 관리지침", "미래 관리지침")
        .replace("20240504", "20260101")
        .replace("</AdmRulService>", "<현행여부>N</현행여부></AdmRulService>");
    fs::create_dir(cache.join("retired")).unwrap();
    fs::write(cache.join("retired/123.xml"), &old).unwrap();
    fs::write(cache.join("124.xml"), &future).unwrap();
    for (stage, serial) in [("before", "123"), ("after", "124")] {
        fs::write(
            cache.join("current_snapshot.json"),
            serde_json::json!({
                "schema_version": 1, "observed_on": "2026-10-09", "rules": {"ABC": serial}
            })
            .to_string(),
        )
        .unwrap();
        let bare = temp.path().join(format!("{stage}.git"));
        let clone = temp.path().join(format!("{stage}-clone"));
        assert!(
            Command::new(env!("CARGO_BIN_EXE_admrule-kr-compiler"))
                .arg(&cache)
                .arg("-o")
                .arg(&bare)
                .status()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .arg("clone")
                .arg(&bare)
                .arg(&clone)
                .status()
                .unwrap()
                .success()
        );
        let python_out = temp.path().join(format!("{stage}-python"));
        assert!(
            Command::new(std::env::var("PYTHON").unwrap_or_else(|_| "python".into()))
                .args(["-m", "admrules.import_admrules", "--repo"])
                .arg(&python_out)
                .env("PYTHONPATH", &pipeline)
                .env("LEGALIZE_ADMRULE_CACHE_DIR", &cache)
                .status()
                .unwrap()
                .success()
        );
        let files = Command::new("git")
            .arg("-C")
            .arg(&clone)
            .args(["-c", "core.quotePath=false", "ls-files"])
            .output()
            .unwrap();
        let files = String::from_utf8(files.stdout).unwrap();
        let bodies: Vec<_> = files.lines().filter(|p| p.ends_with("/본문.md")).collect();
        assert_eq!(bodies.len(), 1);
        for path in bodies {
            assert_eq!(
                fs::read(clone.join(path)).unwrap(),
                fs::read(python_out.join(path)).unwrap()
            );
            assert_eq!(path.contains("미래 관리지침"), stage == "after");
        }
        let history = Command::new("git")
            .arg("-C")
            .arg(&clone)
            .args(["log", "--format=%B"])
            .output()
            .unwrap();
        let history = String::from_utf8(history.stdout).unwrap();
        assert!(history.contains("행정규칙일련번호: 123"));
        assert!(history.contains("행정규칙일련번호: 124"));
    }
}

#[test]
fn invalid_snapshot_fails_before_either_compiler_writes_output() {
    let pipeline = pipeline_dir().expect("Python pipeline required for snapshot parity");
    let temp = tempfile::tempdir().unwrap();
    let cache = temp.path().join("cache");
    fs::create_dir(&cache).unwrap();
    fs::write(cache.join("123.xml"), SAMPLE_XML).unwrap();
    let valid =
        serde_json::json!({"schema_version":1,"observed_on":"2026-10-09","rules":{"ABC":"123"}});
    let mut cases = vec![serde_json::json!([])];
    for (key, value) in [
        ("schema_version", serde_json::json!(2)),
        ("schema_version", serde_json::json!(true)),
        ("rules", serde_json::json!({})),
        ("rules", serde_json::json!([])),
        ("rules", serde_json::json!({"ABC":123})),
        ("rules", serde_json::json!({"ABC":""})),
        ("rules", serde_json::json!({"ABC":"../123"})),
        ("rules", serde_json::json!({"ABC":"１２３"})),
        ("rules", serde_json::json!({"ABC":"999"})),
        ("rules", serde_json::json!({"WRONG":"123"})),
        ("observed_on", serde_json::json!("20261009")),
        ("observed_on", serde_json::json!("2026-1-9")),
        ("observed_on", serde_json::json!("2026-02-30")),
        ("observed_on", serde_json::json!("0000-01-01")),
    ] {
        let mut value_case = valid.clone();
        value_case[key] = value;
        cases.push(value_case);
    }
    for (index, case) in cases.iter().enumerate() {
        fs::write(cache.join("current_snapshot.json"), case.to_string()).unwrap();
        let bare = temp.path().join(format!("invalid-{index}.git"));
        let rust = Command::new(env!("CARGO_BIN_EXE_admrule-kr-compiler"))
            .arg(&cache)
            .arg("-o")
            .arg(&bare)
            .output()
            .unwrap();
        assert!(!rust.status.success(), "Rust accepted {case}");
        assert!(!bare.exists(), "Rust wrote output for {case}");
        let python_out = temp.path().join(format!("invalid-{index}-python"));
        let python = Command::new(std::env::var("PYTHON").unwrap_or_else(|_| "python".into()))
            .args(["-m", "admrules.import_admrules", "--repo"])
            .arg(&python_out)
            .env("PYTHONPATH", &pipeline)
            .env("LEGALIZE_ADMRULE_CACHE_DIR", &cache)
            .output()
            .unwrap();
        assert!(!python.status.success(), "Python accepted {case}");
        assert!(!python_out.exists(), "Python wrote output for {case}");
    }
}

#[test]
fn partial_probe_ignores_global_snapshot_and_active_xml_wins() {
    let temp = tempfile::tempdir().unwrap();
    let cache = temp.path().join("cache");
    fs::create_dir_all(cache.join("retired")).unwrap();
    fs::write(
        cache.join("retired/123.xml"),
        SAMPLE_XML.replace("공공데이터 관리지침", "오래된 캐시"),
    )
    .unwrap();
    fs::write(cache.join("123.xml"), SAMPLE_XML).unwrap();
    fs::write(cache.join("current_snapshot.json"), "{}").unwrap();
    let output = temp.path().join("output");
    let result = Command::new(env!("CARGO_BIN_EXE_admrule-kr-compiler"))
        .arg(&cache)
        .arg("--tree")
        .args(["--limit", "1", "-o"])
        .arg(&output)
        .output()
        .unwrap();
    assert!(result.status.success());
    let (path, expected) = python_reference(SAMPLE_XML);
    assert_eq!(fs::read_to_string(output.join(path)).unwrap(), expected);
}

#[test]
fn xml_references_and_unicode_article_numbers_match_python() {
    let temp = tempfile::tempdir().unwrap();
    let cache = temp.path().join("cache");
    fs::create_dir(&cache).unwrap();
    let xml = SAMPLE_XML
        .replace(
            "제1조 목적",
            "제１조(목적) A &amp; B&#10;① &lt;용어&gt;&#10;제٢장 조문&#10;가의３. 항목",
        )
        .replace("<![CDATA[수수료]]>", "첨부 &amp; 양식\r\n다음 줄");
    fs::write(cache.join("123.xml"), &xml).unwrap();
    let output = temp.path().join("output");
    let result = Command::new(env!("CARGO_BIN_EXE_admrule-kr-compiler"))
        .arg(&cache)
        .arg("--tree")
        .arg("-o")
        .arg(&output)
        .output()
        .unwrap();
    assert!(result.status.success());
    let (path, expected) = python_reference(&xml);
    assert_eq!(fs::read_to_string(output.join(path)).unwrap(), expected);
}

#[test]
fn snapshot_path_swaps_preserve_both_identities_in_bare_repository() {
    let pipeline = pipeline_dir().expect("Python pipeline required for snapshot parity");
    let temp = tempfile::tempdir().unwrap();
    let cache = temp.path().join("cache");
    fs::create_dir(&cache).unwrap();
    for (serial, identity, name, date) in [
        ("123", "ABC", "규칙 A", "20240101"),
        ("124", "DEF", "규칙 B", "20240101"),
        ("125", "ABC", "규칙 B", "20250101"),
        ("126", "DEF", "규칙 A", "20250101"),
    ] {
        fs::write(
            cache.join(format!("{serial}.xml")),
            SAMPLE_XML
                .replace("123", serial)
                .replace("ABC", identity)
                .replace("공공데이터 관리지침", name)
                .replace("20240504", date),
        )
        .unwrap();
    }
    fs::write(
        cache.join("current_snapshot.json"),
        serde_json::json!({
            "schema_version":1,"observed_on":"2026-10-09","rules":{"ABC":"125","DEF":"126"}
        })
        .to_string(),
    )
    .unwrap();
    let bare = temp.path().join("output.git");
    let clone = temp.path().join("clone");
    assert!(
        Command::new(env!("CARGO_BIN_EXE_admrule-kr-compiler"))
            .arg(&cache)
            .arg("-o")
            .arg(&bare)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert!(
        Command::new("git")
            .arg("clone")
            .arg(&bare)
            .arg(&clone)
            .output()
            .unwrap()
            .status
            .success()
    );
    let python_out = temp.path().join("python");
    assert!(
        Command::new(std::env::var("PYTHON").unwrap_or_else(|_| "python".into()))
            .args(["-m", "admrules.import_admrules", "--repo"])
            .arg(&python_out)
            .env("PYTHONPATH", &pipeline)
            .env("LEGALIZE_ADMRULE_CACHE_DIR", &cache)
            .output()
            .unwrap()
            .status
            .success()
    );
    let files = Command::new("git")
        .arg("-C")
        .arg(&clone)
        .args(["-c", "core.quotePath=false", "ls-files"])
        .output()
        .unwrap();
    let files = String::from_utf8(files.stdout).unwrap();
    let bodies: Vec<_> = files
        .lines()
        .filter(|path| path.ends_with("/본문.md"))
        .collect();
    assert_eq!(bodies.len(), 2);
    for path in bodies {
        assert_eq!(
            fs::read(clone.join(path)).unwrap(),
            fs::read(python_out.join(path)).unwrap()
        );
    }
}
