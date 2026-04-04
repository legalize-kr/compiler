use anyhow::{Context, Result};
use quick_xml::Reader;
use quick_xml::escape::unescape;
use quick_xml::events::Event;

#[derive(Debug, Clone, Default)]
/// Metadata extracted from one 법령 XML document.
pub struct LawMetadata {
    /// 법령일련번호 (MST).
    pub mst: String,
    /// 법령명_한글.
    pub law_name: String,
    /// 법령ID.
    pub law_id: String,
    /// 법종구분.
    pub law_type: String,
    /// 법종구분코드.
    pub law_type_code: String,
    /// 소관부처명.
    pub department_name: String,
    /// 공포일자.
    pub promulgation_date: String,
    /// 공포번호.
    pub promulgation_number: String,
    /// 시행일자.
    pub enforcement_date: String,
    /// 제개정구분명.
    pub amendment: String,
    /// 법령분류명.
    pub field: String,
}

#[derive(Debug, Clone, Default)]
/// Lowest-level numbered item inside a subparagraph.
pub struct Item {
    /// Display number such as `가.`.
    pub number: String,
    /// Rendered item body text.
    pub content: String,
}

#[derive(Debug, Clone, Default)]
/// Numbered subdivision inside a paragraph.
pub struct Subparagraph {
    /// Display number such as `1.`.
    pub number: String,
    /// Rendered subparagraph body text.
    pub content: String,
    /// Nested item list.
    pub items: Vec<Item>,
}

#[derive(Debug, Clone, Default)]
/// Paragraph within an article.
pub struct Paragraph {
    /// Display number such as `①`.
    pub number: String,
    /// Rendered paragraph body text.
    pub content: String,
    /// Nested subparagraph list.
    pub subparagraphs: Vec<Subparagraph>,
}

#[derive(Debug, Clone, Default)]
/// Article in the main body of a law.
pub struct Article {
    /// Article number without the `조` suffix.
    pub number: String,
    /// Optional article title.
    pub title: String,
    /// Leading article text before numbered paragraphs.
    pub content: String,
    /// Paragraph list under the article.
    pub paragraphs: Vec<Paragraph>,
}

#[derive(Debug, Clone, Default)]
/// Addendum block from a law document.
pub struct Addendum {
    /// Rendered addendum text.
    pub content: String,
}

#[derive(Debug, Clone, Default)]
/// Fully parsed law document ready for Markdown rendering.
pub struct LawDetail {
    /// Top-level metadata used for file naming and frontmatter.
    pub metadata: LawMetadata,
    /// Parsed article list.
    pub articles: Vec<Article>,
    /// Parsed addenda list.
    pub addenda: Vec<Addendum>,
}

#[derive(Debug, Clone, Default)]
/// Pass-2 article and addendum body extracted from a law XML document.
pub struct LawBody {
    /// Parsed article list.
    pub articles: Vec<Article>,
    /// Parsed addenda list.
    pub addenda: Vec<Addendum>,
}

/// Minimal DOM node used for the full pass-2 XML walk.
#[derive(Debug, Clone)]
struct XmlNode {
    /// Decoded element name.
    name: String,
    /// Concatenated text and CDATA body for this node.
    text: String,
    /// Child elements in source order.
    children: Vec<XmlNode>,
}

impl XmlNode {
    /// Creates an empty node for the named XML element.
    fn new(name: String) -> Self {
        Self {
            name,
            text: String::new(),
            children: Vec::new(),
        }
    }

    /// Returns the direct child text for the first matching element name.
    fn child_text(&self, name: &str) -> String {
        self.children
            .iter()
            .find(|child| child.name == name)
            .map(|child| child.text.clone())
            .unwrap_or_default()
    }

    /// Collects every descendant node whose element name matches `name`.
    fn collect_descendants<'a>(&'a self, name: &str, out: &mut Vec<&'a XmlNode>) {
        if self.name == name {
            out.push(self);
        }
        for child in &self.children {
            child.collect_descendants(name, out);
        }
    }
}

/// Parses only the metadata fields needed for pass-1 ordering and path planning.
pub fn parse_metadata_only(xml: &[u8], mst: &str) -> Result<LawMetadata> {
    //
    // Pass 1 only needs `<기본정보>`, so keep the scan shallow and stop as soon as that section
    // closes instead of paying for the full DOM build used in pass 2.
    //
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(false);

    let mut buf = Vec::new();
    let mut capture_tag: Option<String> = None;
    let mut capture_text = String::new();
    let mut in_basic_info = false;
    let mut metadata = LawMetadata {
        mst: mst.to_owned(),
        law_id: String::new(),
        law_type_code: String::new(),
        amendment: String::new(),
        ..LawMetadata::default()
    };

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) => {
                let tag = decode_name(event.name().as_ref())?;
                if tag == "기본정보" {
                    in_basic_info = true;
                }
                //
                // Mirror the Python search path by only capturing the first matching basic-info
                // field, leaving later duplicates untouched.
                //
                let should_capture = in_basic_info
                    && match tag.as_str() {
                        "법령명_한글" => metadata.law_name.is_empty(),
                        "법령ID" => metadata.law_id.is_empty(),
                        "법종구분" => metadata.law_type.is_empty(),
                        "법종구분코드" => metadata.law_type_code.is_empty(),
                        "공포일자" => metadata.promulgation_date.is_empty(),
                        "공포번호" => metadata.promulgation_number.is_empty(),
                        "시행일자" => metadata.enforcement_date.is_empty(),
                        "소관부처명" => metadata.department_name.is_empty(),
                        "제개정구분명" => metadata.amendment.is_empty(),
                        "법령분류명" => metadata.field.is_empty(),
                        _ => false,
                    };
                if should_capture {
                    capture_text.clear();
                    capture_tag = Some(tag);
                }
            }
            Event::Empty(event) => {
                let tag = decode_name(event.name().as_ref())?;
                //
                // `<기본정보 />` is technically possible, and once the metadata section is over the
                // remaining XML cannot affect pass-1 ordering or path assignment.
                //
                if tag == "기본정보" {
                    break;
                }
            }
            Event::Text(text) => {
                if capture_tag.is_some() {
                    capture_text.push_str(&decode_text(text.as_ref())?);
                }
            }
            Event::CData(text) => {
                if capture_tag.is_some() {
                    capture_text.push_str(&String::from_utf8_lossy(text.as_ref()));
                }
            }
            Event::End(event) => {
                let tag = decode_name(event.name().as_ref())?;
                if let Some(current) = &capture_tag
                    && current == &tag
                {
                    match current.as_str() {
                        "법령명_한글" => metadata.law_name = capture_text.clone(),
                        "법령ID" => metadata.law_id = capture_text.clone(),
                        "법종구분" => metadata.law_type = capture_text.clone(),
                        "법종구분코드" => metadata.law_type_code = capture_text.clone(),
                        "공포일자" => metadata.promulgation_date = capture_text.clone(),
                        "공포번호" => metadata.promulgation_number = capture_text.clone(),
                        "시행일자" => metadata.enforcement_date = capture_text.clone(),
                        "소관부처명" => metadata.department_name = capture_text.clone(),
                        "제개정구분명" => metadata.amendment = capture_text.clone(),
                        "법령분류명" => metadata.field = capture_text.clone(),
                        _ => {}
                    }
                    capture_tag = None;
                }
                //
                // The ordering pass intentionally stops after the first metadata block instead of
                // scanning articles, addenda, or later repeated fields.
                //
                if tag == "기본정보" {
                    break;
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(metadata)
}

/// Parses only the article and addendum body needed during pass 2.
pub fn parse_law_body(xml: &[u8]) -> Result<LawBody> {
    //
    // Build a tiny DOM first so the later extraction logic can mirror ElementTree-style descendant
    // lookups without reparsing the byte stream for every field family.
    //
    let root = {
        let mut reader = Reader::from_reader(xml);
        reader.config_mut().trim_text(false);

        let mut buf = Vec::new();
        let mut stack: Vec<XmlNode> = Vec::new();
        let mut root = None;

        loop {
            match reader.read_event_into(&mut buf)? {
                Event::Start(event) => {
                    let name = decode_name(event.name().as_ref())?;
                    stack.push(XmlNode::new(name));
                }
                Event::Empty(event) => {
                    let name = decode_name(event.name().as_ref())?;
                    let node = XmlNode::new(name);
                    if let Some(parent) = stack.last_mut() {
                        parent.children.push(node);
                    } else {
                        root = Some(node);
                    }
                }
                Event::Text(text) => {
                    if let Some(node) = stack.last_mut() {
                        node.text.push_str(&decode_text(text.as_ref())?);
                    }
                }
                Event::CData(text) => {
                    if let Some(node) = stack.last_mut() {
                        node.text.push_str(&String::from_utf8_lossy(text.as_ref()));
                    }
                }
                Event::End(_) => {
                    let node = stack.pop().context("unexpected end tag")?;
                    if let Some(parent) = stack.last_mut() {
                        parent.children.push(node);
                    } else {
                        root = Some(node);
                    }
                }
                Event::Eof => break,
                _ => {}
            }
            buf.clear();
        }

        root.context("missing XML root")?
    };

    //
    let mut body = LawBody {
        articles: Vec::new(),
        addenda: Vec::new(),
    };

    //
    // Walk article, paragraph, subparagraph, and item descendants into the final nested structs.
    //
    let mut article_nodes = Vec::new();
    root.collect_descendants("조문단위", &mut article_nodes);
    for node in article_nodes {
        let mut article = Article {
            number: node.child_text("조문번호"),
            title: node.child_text("조문제목"),
            content: node.child_text("조문내용"),
            paragraphs: Vec::new(),
        };

        let mut paragraph_nodes = Vec::new();
        node.collect_descendants("항", &mut paragraph_nodes);
        for paragraph_node in paragraph_nodes {
            let mut paragraph = Paragraph {
                number: paragraph_node.child_text("항번호"),
                content: paragraph_node.child_text("항내용"),
                subparagraphs: Vec::new(),
            };

            let mut subparagraph_nodes = Vec::new();
            paragraph_node.collect_descendants("호", &mut subparagraph_nodes);
            for subparagraph_node in subparagraph_nodes {
                let mut subparagraph = Subparagraph {
                    number: subparagraph_node.child_text("호번호"),
                    content: subparagraph_node.child_text("호내용"),
                    items: Vec::new(),
                };

                let mut item_nodes = Vec::new();
                subparagraph_node.collect_descendants("목", &mut item_nodes);
                for item_node in item_nodes {
                    subparagraph.items.push(Item {
                        number: item_node.child_text("목번호"),
                        content: item_node.child_text("목내용"),
                    });
                }

                paragraph.subparagraphs.push(subparagraph);
            }

            article.paragraphs.push(paragraph);
        }

        body.articles.push(article);
    }

    //
    // Collect addenda after the main body so the renderer can append them as a final section.
    //
    let mut addendum_nodes = Vec::new();
    root.collect_descendants("부칙단위", &mut addendum_nodes);
    for node in addendum_nodes {
        body.addenda.push(Addendum {
            content: node.child_text("부칙내용"),
        });
    }

    Ok(body)
}

/// Decodes one XML element name from UTF-8 bytes.
fn decode_name(name: &[u8]) -> Result<String> {
    Ok(std::str::from_utf8(name)?.to_owned())
}

/// Decodes and unescapes one XML text node.
fn decode_text(text: &[u8]) -> Result<String> {
    let text = std::str::from_utf8(text)?;
    Ok(unescape(text)?.into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_metadata_only_like_python_search_path() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<법령>
  <기본정보>
    <법령ID>001566</법령ID>
    <공포일자>19971213</공포일자>
    <공포번호>05453</공포번호>
    <법종구분>법률</법종구분>
    <법종구분코드>010101</법종구분코드>
    <법령명_한글><![CDATA[주세법]]></법령명_한글>
    <시행일자>19980101</시행일자>
    <제개정구분명>일부개정</제개정구분명>
    <연락부서><부서단위><소관부처명>재정경제부</소관부처명></부서단위></연락부서>
  </기본정보>
</법령>"#;

        let metadata = parse_metadata_only(xml.as_bytes(), "5848").unwrap();
        assert_eq!(metadata.law_name, "주세법");
        assert_eq!(metadata.law_type, "법률");
        assert_eq!(metadata.law_type_code, "010101");
        assert_eq!(metadata.amendment, "일부개정");
        assert_eq!(metadata.department_name, "재정경제부");
        assert_eq!(metadata.mst, "5848");
    }

    #[test]
    fn parses_articles_paragraphs_and_items() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<법령>
  <기본정보>
    <법령ID>1</법령ID>
    <법종구분>법률</법종구분>
    <법령명_한글><![CDATA[테스트법]]></법령명_한글>
  </기본정보>
  <조문>
    <조문단위>
      <조문번호>1</조문번호>
      <조문제목><![CDATA[정의]]></조문제목>
      <조문내용><![CDATA[제1조 (정의) 정의한다.]]></조문내용>
      <항>
        <항번호><![CDATA[①]]></항번호>
        <항내용><![CDATA[①첫 문장]]></항내용>
        <호>
          <호번호><![CDATA[1.]]></호번호>
          <호내용><![CDATA[1.  첫 호]]></호내용>
          <목>
            <목번호><![CDATA[가.]]></목번호>
            <목내용><![CDATA[가.  첫 목]]></목내용>
          </목>
        </호>
      </항>
    </조문단위>
  </조문>
</법령>"#;

        let body = parse_law_body(xml.as_bytes()).unwrap();
        assert_eq!(body.articles.len(), 1);
        assert_eq!(body.articles[0].paragraphs.len(), 1);
        assert_eq!(body.articles[0].paragraphs[0].subparagraphs.len(), 1);
        assert_eq!(
            body.articles[0].paragraphs[0].subparagraphs[0].items.len(),
            1
        );
    }
}
