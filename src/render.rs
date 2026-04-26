use std::sync::OnceLock;

use anyhow::{Result, bail};
use regex::Regex;
use rustc_hash::FxHashMap as HashMap;
use serde::Serialize;

use crate::git_repo::RepoPathBuf;
use crate::xml_parser::{LawDetail, LawMetadata};

/// Child-law suffixes that share a parent directory in the output tree.
const CHILD_SUFFIXES: [(&str, &str); 2] = [(" 시행규칙", "시행규칙"), (" 시행령", "시행령")];

/// Classification of a planned entry relative to parent-law grouping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryKind {
    /// A root-level law (법률) or anything else not classified as a child.
    Root,
    /// A 시행령/시행규칙 whose parent group name was derived from the child name.
    Child {
        /// Normalized, space-stripped parent law group name (matches `Root.group`).
        parent_group: String,
    },
}

/// Derived metadata shared by Markdown rendering and commit-message generation.
#[derive(Debug)]
struct PreparedMetadata {
    /// Normalized law title with legacy punctuation rewrites applied.
    normalized_name: String,
    /// Space-stripped title used in law.go.kr URLs.
    compact_name: String,
    /// Raw title before normalization for `원본제목`.
    raw_name: String,
    /// Promulgation date rendered as `YYYY-MM-DD`.
    promulgation_date: String,
    /// Enforcement date rendered as `YYYY-MM-DD`.
    enforcement_date: String,
    /// Promulgation number copied from XML metadata.
    promulgation_number: String,
    /// Department list for YAML frontmatter.
    departments: Vec<String>,
    /// Commit-message department line with the legacy `미상` fallback.
    commit_department: String,
    /// Raw field/category label for YAML frontmatter.
    field: String,
    /// Commit-message field line with the legacy `미분류` fallback.
    commit_field: String,
}

/// Tracks already-assigned output paths so collisions follow the legacy rules.
#[derive(Debug, Default)]
pub struct PathRegistry {
    /// Already assigned paths keyed by the final repository path.
    assigned: HashMap<RepoPathBuf, String>,
    /// Reverse index: law_id → already-assigned path for that law.
    by_id: HashMap<String, RepoPathBuf>,
}

impl PathRegistry {
    /// Returns the Markdown path and entry classification for a law name/type/id triple.
    pub fn get_law_path(
        &mut self,
        law_name: &str,
        law_type: &str,
        law_id: &str,
    ) -> (RepoPathBuf, EntryKind) {
        //
        // Keep the existing repo layout where 시행령/시행규칙 live under the parent law
        // directory instead of getting their own top-level group names.
        //
        let normalized = normalize_law_name(law_name);
        let child_match = CHILD_SUFFIXES.iter().find_map(|(suffix, filename)| {
            normalized
                .strip_suffix(suffix)
                .map(|group| (group, *filename))
        });
        let (group, filename, kind) = if let Some((group, filename)) = child_match {
            let group = group.replace(' ', "");
            //
            // Only promote to Child when the law type actually signals a subordinate 법령. A file
            // that is typed 법률 but happens to end with " 시행령" (e.g. a 법률 whose name matches
            // the suffix) stays classified as Root so orphan detection doesn't misfire.
            //
            let is_child = !law_type.is_empty() && law_type != "법률";
            let kind = if is_child {
                EntryKind::Child {
                    parent_group: group.clone(),
                }
            } else {
                EntryKind::Root
            };
            let filename = if is_child {
                filename.to_owned()
            } else {
                law_type.to_owned()
            };
            (group, filename, kind)
        } else {
            (
                normalized.replace(' ', ""),
                law_type.to_owned(),
                EntryKind::Root,
            )
        };

        //
        // If this law_id already has an assigned path (from an earlier revision), reuse it.
        // This prevents the same law from getting a new qualified path for each ministry rename
        // even when its base path is already occupied by a different law.
        //
        if !law_id.is_empty()
            && let Some(existing_path) = self.by_id.get(law_id)
        {
            return (existing_path.clone(), kind);
        }

        //
        // Reuse the plain `<group>/<filename>.md` path when the law id matches the
        // previous claimant; otherwise append `(법종)` exactly like the legacy repository did.
        //
        let base = RepoPathBuf::kr_file(&group, format!("{filename}.md"));
        if let Some(existing_id) = self.assigned.get(&base)
            && existing_id != law_id
        {
            let qualified = RepoPathBuf::kr_file(&group, format!("{filename}({law_type}).md"));
            self.assigned.insert(qualified.clone(), law_id.to_owned());
            if !law_id.is_empty() {
                self.by_id.insert(law_id.to_owned(), qualified.clone());
            }
            return (qualified, kind);
        }

        self.assigned.insert(base.clone(), law_id.to_owned());
        if !law_id.is_empty() {
            self.by_id.insert(law_id.to_owned(), base.clone());
        }
        (base, kind)
    }
}

/// Normalizes punctuation variants so rendered names match the legacy output.
pub fn normalize_law_name(name: &str) -> String {
    name.chars()
        .map(|ch| match ch {
            '\u{00B7}' | '\u{30FB}' | '\u{FF65}' => '\u{318D}',
            _ => ch,
        })
        .collect()
}

/// Formats a required `YYYYMMDD` date as `YYYY-MM-DD`.
pub fn format_date(date: &str) -> Result<String> {
    if date.len() != 8 || !date.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("expected YYYYMMDD date: {date}");
    }

    Ok(format!("{}-{}-{}", &date[..4], &date[4..6], &date[6..8]))
}

/// Precomputes the normalized metadata view reused across renderer outputs.
fn prepare_metadata(metadata: &LawMetadata) -> Result<PreparedMetadata> {
    //
    // Normalize the law title once and derive the URL-friendly compact title from that same value.
    //
    let normalized_name = normalize_law_name(&metadata.law_name);
    let compact_name = normalized_name.replace(' ', "");

    //
    // Date formatting and department splitting are shared by commit messages and Markdown
    // frontmatter, but only commit messages keep the legacy unknown-field fallbacks.
    //
    let departments = metadata
        .department_name
        .split(',')
        .map(str::trim)
        .filter(|department| !department.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    Ok(PreparedMetadata {
        normalized_name,
        compact_name,
        raw_name: metadata.law_name.clone(),
        promulgation_date: format_date(&metadata.promulgation_date)?,
        enforcement_date: format_date(&metadata.enforcement_date)?,
        promulgation_number: metadata.promulgation_number.clone(),
        commit_department: if metadata.department_name.is_empty() {
            String::from("미상")
        } else {
            metadata.department_name.clone()
        },
        departments,
        field: metadata.field.clone(),
        commit_field: if metadata.field.is_empty() {
            String::from("미분류")
        } else {
            metadata.field.clone()
        },
    })
}

/// Builds the Git commit message for one law revision.
pub fn build_commit_message(metadata: &LawMetadata, mst: &str) -> Result<String> {
    //
    // Normalize display text and project the cache metadata directly into the legacy message shape.
    //
    let prepared = prepare_metadata(metadata)?;
    let title = if metadata.amendment.is_empty() {
        format!("{}: {}", metadata.law_type, prepared.normalized_name)
    } else {
        format!(
            "{}: {} ({})",
            metadata.law_type, prepared.normalized_name, metadata.amendment
        )
    };

    //
    // Assemble the law.go.kr links that appear at the top of every commit message.
    //
    let url_law = format!("https://www.law.go.kr/법령/{}", prepared.compact_name);
    let url_diff = format!(
        "https://www.law.go.kr/법령/신구법비교/{}",
        prepared.compact_name
    );

    //
    // Emit the final line-oriented message body in the historical repository format.
    //
    let mut lines = vec![title, String::new()];
    lines.push(format!("법령 전문: {url_law}"));
    lines.push(format!(
        "제개정문: https://www.law.go.kr/법령/제개정문/{}/({},{})",
        prepared.compact_name, prepared.promulgation_number, metadata.promulgation_date
    ));
    lines.push(format!("신구법비교: {url_diff}"));
    lines.push(String::new());
    lines.push(format!("공포일자: {}", prepared.promulgation_date));
    lines.push(format!("공포번호: {}", prepared.promulgation_number));
    lines.push(format!("소관부처: {}", prepared.commit_department));
    lines.push(format!("법령분야: {}", prepared.commit_field));
    lines.push(format!("법령MST: {mst}"));
    Ok(lines.join("\n"))
}

/// Renders one parsed law document into the repository Markdown format.
pub fn law_to_markdown(detail: &LawDetail) -> Result<Vec<u8>> {
    //
    // Render YAML from the same metadata fields the Python pipeline emits.
    //
    let prepared = prepare_metadata(&detail.metadata)?;
    let frontmatter = {
        Frontmatter {
            title: prepared.normalized_name.clone(),
            mst: detail.metadata.mst.parse::<u64>()?,
            law_id: detail.metadata.law_id.clone(),
            law_type: detail.metadata.law_type.clone(),
            law_type_code: detail.metadata.law_type_code.clone(),
            departments: prepared.departments.clone(),
            promulgation_date: prepared.promulgation_date.clone(),
            promulgation_number: prepared.promulgation_number.clone(),
            enforcement_date: prepared.enforcement_date.clone(),
            field: prepared.field.clone(),
            status: String::from("시행"),
            source: format!("https://www.law.go.kr/법령/{}", prepared.compact_name),
            original_title: (prepared.normalized_name != prepared.raw_name)
                .then_some(prepared.raw_name.clone()),
        }
    };
    let mut yaml = serde_yaml::to_string(&frontmatter)?;
    if let Some(stripped) = yaml.strip_prefix("---\n") {
        yaml = stripped.to_owned();
    }

    //
    // Build the Markdown body from the normalized law title and article structure.
    //
    let mut body_parts = vec![format!("# {}", prepared.normalized_name), String::new()];

    let articles = {
        let mut lines = Vec::new();
        let structure_re = {
            static INSTANCE: OnceLock<Regex> = OnceLock::new();
            INSTANCE.get_or_init(|| Regex::new(r"^제\d+(?:의\d+)?(편|장|절|관)\s*").unwrap())
        };
        let article_prefix_re = {
            static INSTANCE: OnceLock<Regex> = OnceLock::new();
            INSTANCE.get_or_init(|| Regex::new(r"^제\d+조(?:의\d+)?\s*(?:\([^)]*\)\s*)?").unwrap())
        };
        let circled_prefix_re = {
            static INSTANCE: OnceLock<Regex> = OnceLock::new();
            INSTANCE.get_or_init(|| Regex::new(r"^[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳]\s*").unwrap())
        };
        let ho_prefix_re = {
            static INSTANCE: OnceLock<Regex> = OnceLock::new();
            INSTANCE.get_or_init(|| Regex::new(r"^\d+(?:의\d+)?\.\s*").unwrap())
        };
        let mok_prefix_re = {
            static INSTANCE: OnceLock<Regex> = OnceLock::new();
            INSTANCE.get_or_init(|| Regex::new(r"^[가-힣](?:의\d+)?\.\s*").unwrap())
        };

        //
        // Keep the Python-style article, paragraph, subparagraph, and item formatting intact.
        //
        for article in &detail.articles {
            let number = &article.number;
            let branch_number = &article.branch_number;
            let title = &article.title;
            let content = normalize_law_name(article.content.trim());

            if title.is_empty()
                && let Some(captures) = structure_re.captures(&content)
            {
                let level = match captures.get(1).map(|m| m.as_str()) {
                    Some("편") => "#",
                    Some("장") => "##",
                    Some("절") => "###",
                    Some("관") => "####",
                    _ => "",
                };
                if !level.is_empty() {
                    lines.push(format!("{level} {content}"));
                    lines.push(String::new());
                    continue;
                }
            }

            let mut heading = format!("##### 제{number}조");
            if !branch_number.is_empty() {
                heading.push_str(&format!("의{branch_number}"));
            }
            if !title.is_empty() {
                heading.push_str(&format!(" ({title})"));
            }
            lines.push(heading);
            lines.push(String::new());

            if !content.is_empty() {
                let cleaned = article_prefix_re.replace(&content, "").to_string();
                if !cleaned.is_empty() {
                    lines.push(cleaned);
                    lines.push(String::new());
                }
            }

            for paragraph in &article.paragraphs {
                let content = normalize_law_name(&paragraph.content);
                if !content.is_empty() {
                    let stripped = circled_prefix_re.replace(content.trim(), "").to_string();
                    let prefix = if paragraph.number.is_empty() {
                        String::new()
                    } else if paragraph.branch_number.is_empty() {
                        format!("**{}**", paragraph.number)
                    } else {
                        format!("**{}의{}**", paragraph.number, paragraph.branch_number)
                    };
                    lines.push(format!("{prefix} {stripped}"));
                    lines.push(String::new());
                }

                for subparagraph in &paragraph.subparagraphs {
                    let content = normalize_law_name(&subparagraph.content);
                    if !content.is_empty() {
                        let stripped = ho_prefix_re.replace(content.trim(), "").to_string();
                        let stripped = normalize_ws(&stripped);
                        let number = subparagraph.number.trim().trim_end_matches('.');
                        let branch = subparagraph.branch_number.trim();
                        if number.is_empty() {
                            lines.push(format!("  {stripped}"));
                        } else if branch.is_empty() {
                            lines.push(format!("  {number}\\. {stripped}"));
                        } else {
                            lines.push(format!("  {number}의{branch}\\. {stripped}"));
                        }
                    }

                    for item in &subparagraph.items {
                        let content = normalize_law_name(&item.content);
                        if !content.is_empty() {
                            let stripped = mok_prefix_re.replace(content.trim(), "").to_string();
                            let stripped = normalize_ws(&stripped);
                            let number = item.number.trim().trim_end_matches('.');
                            let branch = item.branch_number.trim();
                            if number.is_empty() {
                                lines.push(format!("    {stripped}"));
                            } else if branch.is_empty() {
                                lines.push(format!("    {number}\\. {stripped}"));
                            } else {
                                lines.push(format!("    {number}의{branch}\\. {stripped}"));
                            }
                        }
                    }
                }

                if !paragraph.subparagraphs.is_empty() {
                    lines.push(String::new());
                }
            }
        }

        lines.join("\n")
    };
    if !articles.is_empty() {
        body_parts.push(articles);
    }

    //
    // Append addenda after the main body, trimming only indentation noise from CDATA blocks.
    //
    if !detail.addenda.is_empty() {
        body_parts.push(String::from("## 부칙"));
        body_parts.push(String::new());
        for addendum in &detail.addenda {
            let content = addendum.content.trim();
            if !content.is_empty() {
                // Addenda often arrive indented as CDATA blocks, so strip common leading padding.
                let dedented = {
                    let lines: Vec<&str> = content.lines().collect();
                    let min_indent = lines
                        .iter()
                        .filter_map(|line| {
                            let stripped = line.trim_start();
                            if stripped.is_empty() {
                                None
                            } else {
                                let indent = line.len() - stripped.len();
                                (indent > 0).then_some(indent)
                            }
                        })
                        .min();

                    if let Some(min_indent) = min_indent {
                        lines
                            .into_iter()
                            .map(|line| {
                                let stripped = line.trim_start();
                                if stripped.is_empty() {
                                    String::new()
                                } else {
                                    let indent = line.len() - stripped.len();
                                    let new_indent = indent.saturating_sub(min_indent);
                                    format!("{}{}", " ".repeat(new_indent), stripped)
                                }
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    } else {
                        content.to_owned()
                    }
                };
                body_parts.push(dedented);
                body_parts.push(String::new());
            }
        }
    }

    let body = body_parts.join("\n");
    Ok(format!("---\n{yaml}---\n\n{body}\n").into_bytes())
}

/// Collapses repeated spaces and tabs for rendered list items.
fn normalize_ws(text: &str) -> String {
    static INSTANCE: OnceLock<Regex> = OnceLock::new();
    INSTANCE
        .get_or_init(|| Regex::new(r"[ \t]+").unwrap())
        .replace_all(text, " ")
        .trim()
        .to_owned()
}

/// YAML frontmatter payload for one rendered Markdown file.
#[derive(Debug, Serialize)]
struct Frontmatter {
    /// Display title used as the Markdown heading and `제목`.
    #[serde(rename = "제목")]
    title: String,
    /// MST identifier rendered as the numeric value present in every cache filename.
    #[serde(rename = "법령MST")]
    mst: u64,
    /// Stable law.go.kr law identifier.
    #[serde(rename = "법령ID")]
    law_id: String,
    /// Human-readable law type such as `법률`.
    #[serde(rename = "법령구분")]
    law_type: String,
    /// Machine law type code from the XML payload.
    #[serde(rename = "법령구분코드")]
    law_type_code: String,
    /// Responsible departments split into a YAML list.
    #[serde(rename = "소관부처")]
    departments: Vec<String>,
    /// Normalized promulgation date.
    #[serde(rename = "공포일자")]
    promulgation_date: String,
    /// Promulgation number string from the source XML.
    #[serde(rename = "공포번호")]
    promulgation_number: String,
    /// Normalized enforcement date.
    #[serde(rename = "시행일자")]
    enforcement_date: String,
    /// Law field/category label.
    #[serde(rename = "법령분야")]
    field: String,
    /// Rendered enforcement status.
    #[serde(rename = "상태")]
    status: String,
    /// Canonical law.go.kr source URL.
    #[serde(rename = "출처")]
    source: String,
    /// Original unnormalized title when punctuation had to be rewritten.
    #[serde(rename = "원본제목", skip_serializing_if = "Option::is_none")]
    original_title: Option<String>,
}

#[cfg(test)]
mod tests {
    use crate::xml_parser::Article;
    use crate::xml_parser::{Addendum, Paragraph, Subparagraph};

    use super::*;

    #[test]
    fn path_registry_matches_existing_collision_rule() {
        let mut registry = PathRegistry::default();
        // Different law_ids → genuine collision → qualified path
        let (path, kind) = registry.get_law_path("테스트법 시행규칙", "부령", "ID001");
        assert_eq!(path, RepoPathBuf::kr_file("테스트법", "시행규칙.md"));
        assert_eq!(
            kind,
            EntryKind::Child {
                parent_group: String::from("테스트법"),
            }
        );
        let (path, kind) = registry.get_law_path("테스트법 시행규칙", "총리령", "ID002");
        assert_eq!(
            path,
            RepoPathBuf::kr_file("테스트법", "시행규칙(총리령).md")
        );
        assert_eq!(
            kind,
            EntryKind::Child {
                parent_group: String::from("테스트법"),
            }
        );
    }

    #[test]
    fn child_classification_requires_non_bup_law_type() {
        let mut registry = PathRegistry::default();
        let (_, kind) = registry.get_law_path("테스트 시행령", "법률", "");
        assert_eq!(kind, EntryKind::Root);

        let mut registry = PathRegistry::default();
        let (_, kind) = registry.get_law_path("테스트 시행령", "대통령령", "");
        assert_eq!(
            kind,
            EntryKind::Child {
                parent_group: String::from("테스트"),
            }
        );
    }

    #[test]
    fn path_registry_treats_ministry_rename_as_same_law() {
        let mut registry = PathRegistry::default();
        // Same law_id, different law_type (ministry rename) → same path
        let (path, _) = registry.get_law_path("테스트법 시행규칙", "국토교통부령", "ID001");
        assert_eq!(path, RepoPathBuf::kr_file("테스트법", "시행규칙.md"));
        let (path, _) = registry.get_law_path("테스트법 시행규칙", "행정안전부령", "ID001");
        assert_eq!(path, RepoPathBuf::kr_file("테스트법", "시행규칙.md"));
    }

    #[test]
    fn path_registry_merges_qualified_paths_for_same_law_id() {
        let mut registry = PathRegistry::default();
        // ID001 claims the base path
        let (path, _) = registry.get_law_path("테스트법 시행규칙", "정보통신부령", "ID001");
        assert_eq!(path, RepoPathBuf::kr_file("테스트법", "시행규칙.md"));
        // ID002 (different law) gets a qualified path on first call
        let (path, _) = registry.get_law_path("테스트법 시행규칙", "미래창조과학부령", "ID002");
        assert_eq!(
            path,
            RepoPathBuf::kr_file("테스트법", "시행규칙(미래창조과학부령).md")
        );
        // ID002 with a different ministry rename must reuse the same qualified path
        let (path, _) = registry.get_law_path("테스트법 시행규칙", "과학기술정보통신부령", "ID002");
        assert_eq!(
            path,
            RepoPathBuf::kr_file("테스트법", "시행규칙(미래창조과학부령).md")
        );
    }

    #[test]
    fn markdown_renders_python_style_lists_and_addenda() {
        let detail = LawDetail {
            metadata: LawMetadata {
                mst: String::from("1"),
                law_name: String::from("테스트법"),
                law_id: String::from("000001"),
                law_type: String::from("법률"),
                promulgation_date: String::from("20240101"),
                promulgation_number: String::from("00001"),
                enforcement_date: String::from("20240101"),
                department_name: String::from("법무부"),
                ..LawMetadata::default()
            },
            articles: vec![Article {
                number: String::from("1"),
                branch_number: String::new(),
                title: String::from("정의"),
                content: String::from("제1조 (정의) 본문"),
                paragraphs: vec![Paragraph {
                    number: String::from("①"),
                    branch_number: String::new(),
                    content: String::from("①정의"),
                    subparagraphs: vec![Subparagraph {
                        number: String::from("1."),
                        branch_number: String::new(),
                        content: String::from("1.  첫 호"),
                        items: vec![crate::xml_parser::Item {
                            number: String::from("가."),
                            branch_number: String::new(),
                            content: String::from("가.  첫 목"),
                        }],
                    }],
                }],
            }],
            addenda: vec![Addendum {
                content: String::from("    부칙 본문"),
            }],
        };

        let markdown = String::from_utf8(law_to_markdown(&detail).unwrap()).unwrap();
        assert!(markdown.contains("##### 제1조 (정의)"));
        assert!(markdown.contains("  1\\. 첫 호"));
        assert!(markdown.contains("    가\\. 첫 목"));
        assert!(markdown.contains("## 부칙"));
    }

    #[test]
    fn markdown_renders_branch_numbers_for_jo_hang_ho_mok() {
        //
        // Regression for legalize-kr/legalize-pipeline#2: 조문가지번호·호가지번호가 드롭되어
        // `제4조의2`가 `제4조`로, `제1호의2`가 `1.`로 렌더되던 문제. 항/목 가지번호는 현재
        // 업스트림에 나타나지 않지만 파서·렌더가 대칭적으로 처리해야 한다.
        //
        let detail = LawDetail {
            metadata: LawMetadata {
                mst: String::from("1"),
                law_name: String::from("테스트법"),
                law_id: String::from("000001"),
                law_type: String::from("법률"),
                promulgation_date: String::from("20240101"),
                promulgation_number: String::from("00001"),
                enforcement_date: String::from("20240101"),
                department_name: String::from("법무부"),
                ..LawMetadata::default()
            },
            articles: vec![Article {
                number: String::from("4"),
                branch_number: String::from("2"),
                title: String::from("가지조"),
                content: String::from("제4조의2 (가지조) 본문"),
                paragraphs: vec![Paragraph {
                    number: String::from("①"),
                    branch_number: String::from("3"),
                    content: String::from("①가지항"),
                    subparagraphs: vec![Subparagraph {
                        number: String::from("1."),
                        branch_number: String::from("2"),
                        content: String::from("1의2.  가지호"),
                        items: vec![crate::xml_parser::Item {
                            number: String::from("가."),
                            branch_number: String::from("4"),
                            content: String::from("가의4.  가지목"),
                        }],
                    }],
                }],
            }],
            addenda: Vec::new(),
        };

        let markdown = String::from_utf8(law_to_markdown(&detail).unwrap()).unwrap();
        assert!(markdown.contains("##### 제4조의2 (가지조)"), "article heading missing 의2: {markdown}");
        assert!(markdown.contains("**①의3**"), "paragraph prefix missing 의3: {markdown}");
        assert!(markdown.contains("  1의2\\. 가지호"), "subparagraph prefix missing 의2: {markdown}");
        assert!(markdown.contains("    가의4\\. 가지목"), "item prefix missing 의4: {markdown}");
    }

    #[test]
    fn markdown_rejects_non_compact_promulgation_dates() {
        let detail = LawDetail {
            metadata: LawMetadata {
                mst: String::from("1"),
                law_name: String::from("테스트법"),
                promulgation_date: String::from("2024-01-01"),
                enforcement_date: String::from("20240101"),
                ..LawMetadata::default()
            },
            ..LawDetail::default()
        };

        let error = law_to_markdown(&detail).unwrap_err();
        assert!(error.to_string().contains("YYYYMMDD"));
    }

    #[test]
    fn markdown_keeps_empty_departments_as_empty_list() {
        let detail = LawDetail {
            metadata: LawMetadata {
                mst: String::from("1"),
                law_name: String::from("테스트법"),
                law_id: String::from("000001"),
                law_type: String::from("법률"),
                promulgation_date: String::from("20240101"),
                promulgation_number: String::from("00001"),
                enforcement_date: String::from("20240101"),
                department_name: String::new(),
                ..LawMetadata::default()
            },
            ..LawDetail::default()
        };

        let markdown = String::from_utf8(law_to_markdown(&detail).unwrap()).unwrap();
        assert!(markdown.contains("소관부처: []"));
        assert!(!markdown.contains("소관부처:\n- ''"));
    }

    #[test]
    fn commit_message_keeps_unknown_department_and_field_fallbacks() {
        let metadata = LawMetadata {
            law_name: String::from("테스트법"),
            law_type: String::from("법률"),
            promulgation_date: String::from("20240101"),
            promulgation_number: String::from("00001"),
            enforcement_date: String::from("20240101"),
            amendment: String::from("일부개정"),
            ..LawMetadata::default()
        };

        let message = build_commit_message(&metadata, "1").unwrap();
        assert!(message.contains("소관부처: 미상"));
        assert!(message.contains("법령분야: 미분류"));
    }

    #[test]
    fn commit_message_omits_empty_amendment_suffix() {
        let metadata = LawMetadata {
            law_name: String::from("테스트법"),
            law_type: String::from("법률"),
            promulgation_date: String::from("20240101"),
            promulgation_number: String::from("00001"),
            enforcement_date: String::from("20240101"),
            amendment: String::new(),
            ..LawMetadata::default()
        };

        let message = build_commit_message(&metadata, "1").unwrap();
        assert!(message.starts_with("법률: 테스트법\n"));
        assert!(!message.starts_with("법률: 테스트법 ()\n"));
    }
}
