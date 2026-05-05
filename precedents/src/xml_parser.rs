//! XML parsing helpers for cached law.go.kr precedent documents.

use anyhow::{Context, Result};
use quick_xml::Reader;
use quick_xml::escape::unescape;
use quick_xml::events::Event;

/// Sentinel filled into the composite filename when 선고일자 is missing or invalid.
///
/// Frontmatter still omits the `선고일자` key in that case (see `render.rs:Frontmatter`);
/// this sentinel only protects the filename grammar slot from collapsing.
pub const MISSING_DATE_SENTINEL: &str = "0000-00-00";

/// Sentinel filled into the composite filename when 법원명 is missing.
///
/// When this branch fires, `compose_filename_stem` also forces CASENO to `serial`
/// because a composite key without a real court has lost its discriminator.
pub const MISSING_COURT_SENTINEL: &str = "미상법원";

/// Metadata extracted from one `PrecService` XML document.
#[derive(Debug, Clone, Default)]
pub struct PrecedentMetadata {
    /// 판례정보일련번호 (also used as the cache filename stem).
    pub serial: String,
    /// 사건번호 (raw, may include parentheses and commas).
    pub case_no: String,
    /// 사건명.
    pub case_name: String,
    /// 법원명 (raw, normalization happens in the renderer).
    pub court_name: String,
    /// 법원종류코드.
    pub court_code: String,
    /// 선고일자 in `YYYYMMDD` form (may be empty when upstream omits it).
    pub judgment_date: String,
    /// 사건종류명 (raw, normalization happens in the renderer).
    pub case_type_raw: String,
}

/// Body fields that drive the rendered Markdown sections.
#[derive(Debug, Clone, Default)]
pub struct PrecedentBody {
    /// 판시사항.
    pub ruling_matters: String,
    /// 판결요지.
    pub ruling_summary: String,
    /// 참조조문.
    pub referenced_laws: String,
    /// 참조판례.
    pub referenced_cases: String,
    /// 판례내용.
    pub full_text: String,
}

/// Fully parsed precedent document ready for Markdown rendering.
#[derive(Debug, Clone, Default)]
pub struct PrecedentDetail {
    /// Top-level metadata used for path planning and frontmatter.
    pub metadata: PrecedentMetadata,
    /// Body sections appended to the rendered Markdown.
    pub body: PrecedentBody,
}

/// Parses only the metadata fields needed for pass-1 ordering and path planning.
///
/// Returns `Ok(None)` when the document is not a `PrecService` payload (for example,
/// upstream HTML error pages), the cache filename serial is empty, or the XML omits
/// `판례정보일련번호`.
pub fn parse_metadata_only(xml: &[u8], serial: &str) -> Result<Option<PrecedentMetadata>> {
    if serial.is_empty() {
        return Ok(None);
    }

    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(false);

    let mut buf = Vec::new();
    let mut capture_tag: Option<String> = None;
    let mut capture_text = String::new();
    let mut root_seen = false;
    let mut metadata = PrecedentMetadata::default();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) => {
                let tag = decode_name(event.name().as_ref())?;
                if !root_seen {
                    if tag != "PrecService" {
                        return Ok(None);
                    }
                    root_seen = true;
                    buf.clear();
                    continue;
                }
                //
                // Mirror the Python search path by only capturing the first matching field, leaving
                // later duplicates untouched.
                //
                let should_capture = match tag.as_str() {
                    "판례정보일련번호" => metadata.serial.is_empty(),
                    "사건번호" => metadata.case_no.is_empty(),
                    "사건명" => metadata.case_name.is_empty(),
                    "법원명" => metadata.court_name.is_empty(),
                    "법원종류코드" => metadata.court_code.is_empty(),
                    "선고일자" => metadata.judgment_date.is_empty(),
                    "사건종류명" => metadata.case_type_raw.is_empty(),
                    _ => false,
                };
                if should_capture {
                    capture_text.clear();
                    capture_tag = Some(tag);
                }
            }
            Event::Empty(event) => {
                let tag = decode_name(event.name().as_ref())?;
                if !root_seen {
                    if tag != "PrecService" {
                        return Ok(None);
                    }
                    return Ok(None);
                }
            }
            Event::Text(text) if capture_tag.is_some() => {
                capture_text.push_str(&decode_text(text.as_ref())?);
            }
            Event::CData(text) if capture_tag.is_some() => {
                capture_text.push_str(&String::from_utf8_lossy(text.as_ref()));
            }
            Event::End(event) => {
                let tag = decode_name(event.name().as_ref())?;
                if let Some(current) = &capture_tag
                    && current == &tag
                {
                    match current.as_str() {
                        "판례정보일련번호" => metadata.serial = capture_text.clone(),
                        "사건번호" => metadata.case_no = capture_text.clone(),
                        "사건명" => metadata.case_name = capture_text.clone(),
                        "법원명" => metadata.court_name = capture_text.clone(),
                        "법원종류코드" => metadata.court_code = capture_text.clone(),
                        "선고일자" => {
                            metadata.judgment_date = normalize_dangi_yyyymmdd(&capture_text);
                        }
                        "사건종류명" => metadata.case_type_raw = capture_text.clone(),
                        _ => {}
                    }
                    capture_tag = None;
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    if !root_seen {
        return Ok(None);
    }
    if metadata.serial.is_empty() {
        return Ok(None);
    }
    if metadata.serial != serial {
        anyhow::bail!(
            "판례정보일련번호 {} does not match cache filename serial {serial}",
            metadata.serial
        );
    }
    Ok(Some(metadata))
}

/// Parses the five Markdown body fields out of a `PrecService` document.
pub fn parse_precedent_body(xml: &[u8]) -> Result<PrecedentBody> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(false);

    let mut buf = Vec::new();
    let mut capture_tag: Option<String> = None;
    let mut capture_text = String::new();
    let mut body = PrecedentBody::default();
    let mut root_seen = false;

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) => {
                let tag = decode_name(event.name().as_ref())?;
                if !root_seen {
                    root_seen = true;
                    buf.clear();
                    continue;
                }
                let should_capture = matches!(
                    tag.as_str(),
                    "판시사항" | "판결요지" | "참조조문" | "참조판례" | "판례내용"
                );
                if should_capture {
                    capture_text.clear();
                    capture_tag = Some(tag);
                }
            }
            Event::Empty(_) if !root_seen => {
                root_seen = true;
            }
            Event::Text(text) if capture_tag.is_some() => {
                capture_text.push_str(&decode_text(text.as_ref())?);
            }
            Event::CData(text) if capture_tag.is_some() => {
                capture_text.push_str(&String::from_utf8_lossy(text.as_ref()));
            }
            Event::End(event) => {
                let tag = decode_name(event.name().as_ref())?;
                if let Some(current) = &capture_tag
                    && current == &tag
                {
                    match current.as_str() {
                        "판시사항" => body.ruling_matters = capture_text.clone(),
                        "판결요지" => body.ruling_summary = capture_text.clone(),
                        "참조조문" => body.referenced_laws = capture_text.clone(),
                        "참조판례" => body.referenced_cases = capture_text.clone(),
                        "판례내용" => body.full_text = capture_text.clone(),
                        _ => {}
                    }
                    capture_tag = None;
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(body)
}

/// Decodes one XML element name from UTF-8 bytes.
fn decode_name(name: &[u8]) -> Result<String> {
    Ok(std::str::from_utf8(name)
        .context("element name is not valid UTF-8")?
        .to_owned())
}

/// Decodes and unescapes one XML text node.
fn decode_text(text: &[u8]) -> Result<String> {
    let text = std::str::from_utf8(text).context("text node is not valid UTF-8")?;
    Ok(unescape(text)?.into_owned())
}

/// Dangi era → Gregorian offset (CE = Dangi − 2333).
const DANGI_EPOCH_OFFSET: u32 = 2333;
/// Dangi year floor that covers every realistic Korean legal precedent (≈1867 CE).
const DANGI_YEAR_MIN: u32 = 4200;
/// Dangi year ceiling (≈1997 CE); newer records are always emitted in Gregorian upstream.
const DANGI_YEAR_MAX: u32 = 4330;

/// Normalizes a `YYYYMMDD` 선고일자 so Dangi-era years become Gregorian.
///
/// Older upstream precedents (예: 1956년 선고) are delivered with a 4-digit 단기 연도
/// (`42890525`) instead of 서기 (`19560525`). Converting here, at parse time, makes
/// downstream sorting, commit timestamps, and frontmatter all agree on Gregorian.
/// Non-Dangi inputs, blanks, and malformed strings pass through untouched.
pub fn normalize_dangi_yyyymmdd(date: &str) -> String {
    if date.len() != 8 || !date.bytes().all(|b| b.is_ascii_digit()) {
        return date.to_owned();
    }
    let Ok(year) = date[..4].parse::<u32>() else {
        return date.to_owned();
    };
    if !(DANGI_YEAR_MIN..=DANGI_YEAR_MAX).contains(&year) {
        return date.to_owned();
    }
    format!("{:04}{}", year - DANGI_EPOCH_OFFSET, &date[4..])
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_PREC_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<PrecService>
  <판례정보일련번호>145683</판례정보일련번호>
  <사건명><![CDATA[손해배상(사실혼파기)]]></사건명>
  <사건번호><![CDATA[2000므1257(본소), 1264(반소)]]></사건번호>
  <선고일자>20031114</선고일자>
  <선고>선고</선고>
  <법원명>대법원</법원명>
  <법원종류코드>400201</법원종류코드>
  <사건종류명>가사</사건종류명>
  <사건종류코드>400101</사건종류코드>
  <판결유형>판결</판결유형>
  <판시사항><![CDATA[<br/>판시사항 본문<br/>]]></판시사항>
  <판결요지><![CDATA[판결요지 본문]]></판결요지>
  <참조조문><![CDATA[참조조문 본문]]></참조조문>
  <참조판례><![CDATA[참조판례 본문]]></참조판례>
  <판례내용><![CDATA[<p>판례내용 본문</p>]]></판례내용>
</PrecService>"#;

    #[test]
    fn parses_metadata_for_prec_service_xml() {
        let metadata = parse_metadata_only(SAMPLE_PREC_XML.as_bytes(), "145683")
            .unwrap()
            .unwrap();
        assert_eq!(metadata.serial, "145683");
        assert_eq!(metadata.case_no, "2000므1257(본소), 1264(반소)");
        assert_eq!(metadata.case_name, "손해배상(사실혼파기)");
        assert_eq!(metadata.court_name, "대법원");
        assert_eq!(metadata.court_code, "400201");
        assert_eq!(metadata.judgment_date, "20031114");
        assert_eq!(metadata.case_type_raw, "가사");
    }

    #[test]
    fn returns_none_for_non_prec_service_root() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?><Law>일치하는 판례가 없습니다</Law>"#;
        assert!(parse_metadata_only(xml.as_bytes(), "1").unwrap().is_none());
    }

    #[test]
    fn returns_none_for_empty_serial() {
        assert!(
            parse_metadata_only(SAMPLE_PREC_XML.as_bytes(), "")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn returns_none_when_xml_serial_is_missing() {
        let xml = SAMPLE_PREC_XML.replace("  <판례정보일련번호>145683</판례정보일련번호>\n", "");
        assert!(
            parse_metadata_only(xml.as_bytes(), "145683")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_xml_serial_mismatch() {
        let error = parse_metadata_only(SAMPLE_PREC_XML.as_bytes(), "999999").unwrap_err();
        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn normalizes_dangi_year_in_judgment_date() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<PrecService>
  <판례정보일련번호>232199</판례정보일련번호>
  <사건번호><![CDATA[4289행5]]></사건번호>
  <선고일자>42890525</선고일자>
  <법원명>서울고법</법원명>
  <법원종류코드>400202</법원종류코드>
  <사건종류명>일반행정</사건종류명>
</PrecService>"#;
        let metadata = parse_metadata_only(xml.as_bytes(), "232199")
            .unwrap()
            .unwrap();
        assert_eq!(metadata.judgment_date, "19560525");
        assert_eq!(metadata.case_no, "4289행5");
    }

    #[test]
    fn dangi_normalization_passes_through_gregorian_dates() {
        assert_eq!(normalize_dangi_yyyymmdd("20240101"), "20240101");
        assert_eq!(normalize_dangi_yyyymmdd(""), "");
        assert_eq!(normalize_dangi_yyyymmdd("abcd0101"), "abcd0101");
        assert_eq!(normalize_dangi_yyyymmdd("00000000"), "00000000");
    }

    #[test]
    fn dangi_normalization_boundary_years() {
        assert_eq!(normalize_dangi_yyyymmdd("42000101"), "18670101");
        assert_eq!(normalize_dangi_yyyymmdd("43301231"), "19971231");
        assert_eq!(normalize_dangi_yyyymmdd("41991231"), "41991231");
        assert_eq!(normalize_dangi_yyyymmdd("43310101"), "43310101");
    }

    #[test]
    fn parses_body_fields() {
        let body = parse_precedent_body(SAMPLE_PREC_XML.as_bytes()).unwrap();
        assert!(body.ruling_matters.contains("판시사항 본문"));
        assert_eq!(body.ruling_summary, "판결요지 본문");
        assert_eq!(body.referenced_laws, "참조조문 본문");
        assert_eq!(body.referenced_cases, "참조판례 본문");
        assert!(body.full_text.contains("판례내용 본문"));
    }
}
