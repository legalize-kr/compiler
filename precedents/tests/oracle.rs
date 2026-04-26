//! Cross-language oracle test: byte-equality against Python `compose_filename_stem`.
//!
//! Reads a JSONL produced by `legalize-pipeline/precedents/dump_oracle.py` and asserts
//! that the Rust `compose_filename_stem` returns the exact same stem for every record.
//! Source of truth for stems is Python; Rust must match byte-for-byte.
//!
//! Lookup order for the JSONL file (first hit wins):
//!   1. `ORACLE_PATH` env var
//!   2. `/tmp/oracle.jsonl`
//!   3. `../oracle.jsonl`
//!
//! When no oracle file exists, the full-data test is skipped (printed warning), so
//! `cargo test` stays green during phase rollout. A small synthetic-fixture test
//! still runs unconditionally to catch obvious schema/format regressions.

use std::path::PathBuf;

use precedent_kr_compiler::render::compose_filename_stem;
use serde::Deserialize;

/// One line of the oracle JSONL — schema mirrors `dump_oracle.py`.
#[derive(Debug, Deserialize)]
struct OracleRecord {
    serial: String,
    court: String,
    /// 선고일자 in raw `YYYYMMDD` form (the same value that hits `compose_filename_stem`).
    date: String,
    caseno: String,
    expected_stem: String,
}

fn oracle_path() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("ORACLE_PATH") {
        let pb = PathBuf::from(p);
        if pb.exists() {
            return Some(pb);
        }
    }
    for cand in ["/tmp/oracle.jsonl", "../oracle.jsonl"] {
        let pb = PathBuf::from(cand);
        if pb.exists() {
            return Some(pb);
        }
    }
    None
}

#[test]
fn oracle_byte_equality_full_dataset() {
    let Some(path) = oracle_path() else {
        eprintln!(
            "[oracle] no oracle.jsonl found (set ORACLE_PATH, or write to /tmp/oracle.jsonl). \
             Skipping full-dataset oracle. Synthetic fixture still runs."
        );
        return;
    };
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read oracle {}: {e}", path.display()));

    let mut total = 0usize;
    let mut mismatches: Vec<(OracleRecord, String)> = Vec::new();
    for (lineno, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let rec: OracleRecord = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("parse line {} in {}: {e}", lineno + 1, path.display()));
        let actual = compose_filename_stem(&rec.court, &rec.date, &rec.caseno, &rec.serial);
        total += 1;
        if actual != rec.expected_stem {
            if mismatches.len() < 10 {
                mismatches.push((rec, actual));
            } else {
                // count-only past 10
                mismatches.push((
                    OracleRecord {
                        serial: String::new(),
                        court: String::new(),
                        date: String::new(),
                        caseno: String::new(),
                        expected_stem: String::new(),
                    },
                    String::new(),
                ));
            }
        }
    }

    if !mismatches.is_empty() {
        let shown = mismatches.iter().take(10);
        for (rec, actual) in shown {
            eprintln!(
                "[oracle MISMATCH] serial={} court={:?} date={:?} caseno={:?}\n  expected={:?}\n  actual  ={:?}",
                rec.serial, rec.court, rec.date, rec.caseno, rec.expected_stem, actual,
            );
        }
        panic!(
            "{}/{} oracle mismatches against {}",
            mismatches.len(),
            total,
            path.display()
        );
    }
    eprintln!("[oracle] {total} records OK against {}", path.display());
}

#[test]
fn oracle_synthetic_fixture_byte_equality() {
    // Tiny hand-rolled fixture matching the JSONL schema. Verifies the test loader
    // and Rust compose_filename_stem agree on the spec'd grammar without needing
    // the full Python dump.
    let lines = [
        // happy path
        r#"{"serial":"100","court":"대법원","date":"20030310","caseno":"2002다56116","expected_stem":"대법원_2003-03-10_2002다56116"}"#,
        // 병합/분리
        r#"{"serial":"145683","court":"대법원","date":"20031114","caseno":"2000므1257(본소), 1264(반소)","expected_stem":"대법원_2003-11-14_2000므1257_본소_1264_반소"}"#,
        // missing date → 0000-00-00
        r#"{"serial":"100","court":"대법원","date":"","caseno":"2024가합1","expected_stem":"대법원_0000-00-00_2024가합1"}"#,
        // missing court → 미상법원 + caseno=serial
        r#"{"serial":"999","court":"","date":"20240101","caseno":"2024가합1","expected_stem":"미상법원_2024-01-01_999"}"#,
        // court abbreviation expansion
        r#"{"serial":"42","court":"서울고법","date":"20200101","caseno":"2019나1","expected_stem":"서울고등법원_2020-01-01_2019나1"}"#,
    ];
    for line in lines {
        let rec: OracleRecord = serde_json::from_str(line).expect("parse synthetic line");
        let actual = compose_filename_stem(&rec.court, &rec.date, &rec.caseno, &rec.serial);
        assert_eq!(
            actual, rec.expected_stem,
            "synthetic fixture mismatch for serial={} court={:?} date={:?} caseno={:?}",
            rec.serial, rec.court, rec.date, rec.caseno,
        );
    }
}
