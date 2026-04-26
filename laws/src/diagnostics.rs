//! Structured diagnostics gathered during planning and optionally exposed to operators.
//!
//! Planner anomalies (unparsable XML, empty metadata, orphan child laws) flow into
//! [`Diagnostics`], which feeds both the `--validate` JSON report and the post-build
//! `--manifest` payload.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

use rustc_hash::FxHashMap as HashMap;

/// Record for one cached XML file that failed the planning pass.
#[derive(Serialize, Debug, Clone)]
pub struct UnparsableRecord {
    /// Source file name (basename) of the offending XML document.
    pub file: String,
    /// Human-readable parser error message.
    pub error: String,
}

/// Record for one planned child entry whose parent 법률 is missing from the cache.
#[derive(Serialize, Debug, Clone)]
pub struct OrphanRecord {
    /// Original law name reported by the child entry.
    pub law_name: String,
    /// Law-type label of the orphan child (e.g. 대통령령).
    pub law_type: String,
    /// Normalized parent group name that was expected but not found.
    pub parent_group: String,
}

/// Accumulated planning diagnostics shared by validate and build paths.
#[derive(Serialize, Debug, Default)]
pub struct Diagnostics {
    /// Cache files whose XML failed to parse.
    pub unparsable: Vec<UnparsableRecord>,
    /// Cache file names whose basic metadata block was empty.
    pub empty_metadata: Vec<String>,
    /// Planned child entries whose parent 법률 could not be located.
    pub orphan_children: Vec<OrphanRecord>,
    /// Count of planned entries keyed by law_type.
    pub by_type: HashMap<String, usize>,
    /// Total XML files observed in the detail cache directory.
    pub total_xml: usize,
}

impl Diagnostics {
    /// Returns true when no anomalies were collected.
    pub fn is_clean(&self) -> bool {
        self.unparsable.is_empty()
            && self.empty_metadata.is_empty()
            && self.orphan_children.is_empty()
    }
}

/// Pre-flight validation report emitted via stdout when `--validate` is set.
#[derive(Serialize, Debug)]
pub struct ValidationReport<'a> {
    /// Total XML files observed in the detail cache directory.
    pub total_xml: usize,
    /// Cache files whose XML failed to parse.
    pub unparsable: &'a [UnparsableRecord],
    /// Cache file names whose basic metadata block was empty.
    pub empty_metadata: &'a [String],
    /// Planned child entries whose parent 법률 could not be located.
    pub orphan_children: &'a [OrphanRecord],
    /// Planned entry counts keyed by law_type.
    pub by_type: &'a HashMap<String, usize>,
    /// Operator-supplied expected law total (via `--expect-laws`), if any.
    pub expected_laws: Option<usize>,
    /// Actual planned entry count after pass 1.
    pub actual_laws: usize,
}

/// Build manifest JSON written to disk after a successful build.
#[derive(Serialize, Debug)]
pub struct BuildManifest<'a> {
    /// Manifest schema version.
    pub schema_version: u32,
    /// HEAD commit SHA of the finished bare repository (lowercase hex, 40 chars).
    pub head_commit_sha: String,
    /// Total number of planned entries committed into the output repository.
    pub entries_total: usize,
    /// Cache files whose XML failed to parse.
    pub unparsable: &'a [UnparsableRecord],
    /// Cache file names whose basic metadata block was empty.
    pub empty_metadata: &'a [String],
    /// Planned child entries whose parent 법률 could not be located.
    pub orphan_children: &'a [OrphanRecord],
    /// Planned entry counts keyed by law_type.
    pub by_type: &'a HashMap<String, usize>,
}

/// Writes a pretty-printed manifest JSON payload to the given path.
pub fn write_manifest(path: &Path, manifest: &BuildManifest) -> Result<()> {
    let json = serde_json::to_string_pretty(manifest)
        .context("failed to serialize build manifest as JSON")?;
    std::fs::write(path, json)
        .with_context(|| format!("failed to write manifest to {}", path.display()))?;
    Ok(())
}
