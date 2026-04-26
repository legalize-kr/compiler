//! Property tests for `compose_filename_stem` (mirrors Python Hypothesis suite).
//!
//! Invariants:
//!   - NFC idempotence: composing twice (input already NFC) yields the same stem.
//!   - Determinism: same input → same output.
//!   - Grammar regex: the stem matches `^[^/]+SEP\d{4}-\d{2}-\d{2}SEP[^/]+$`.
//!   - SEP guard: `sanitize_case_number` never produces `SEP` in its output.
//!
//! Input charsets exclude `_` because real Korean 사건번호 strings only contain
//! Hangul, digits, and `(),` separators (preflight (7) of the plan validates this
//! against `.cache/precedent/*.xml`). Adversarial inputs containing `_` would
//! trigger the SEP-decision gate (§1.1.1) and switch SEP to `~`, which is a
//! separate rollout step driven by the preflight measurement.

use precedent_kr_compiler::render::{SEP, compose_filename_stem, sanitize_case_number};
use proptest::prelude::*;
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

fn grammar_re() -> Regex {
    // SEP is a constant string; embed as literal escape.
    let escaped = regex::escape(SEP);
    Regex::new(&format!(
        "^[^/]+{escaped}\\d{{4}}-\\d{{2}}-\\d{{2}}{escaped}[^/]+$"
    ))
    .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn stem_is_deterministic(
        court in "[a-zA-Z가-힣]{0,20}",
        yyyymmdd in "[0-9]{0,8}",
        caseno in "[가-힣0-9()]{0,40}",
        serial in "[0-9]{1,7}",
    ) {
        let a = compose_filename_stem(&court, &yyyymmdd, &caseno, &serial);
        let b = compose_filename_stem(&court, &yyyymmdd, &caseno, &serial);
        prop_assert_eq!(a, b);
    }

    #[test]
    fn stem_matches_grammar(
        court in "[가-힣]{1,10}",
        y in 1900u32..2100,
        m in 1u32..=12,
        d in 1u32..=28,
        caseno in "[가-힣0-9]{1,20}",
        serial in "[0-9]{1,7}",
    ) {
        let date = format!("{y:04}{m:02}{d:02}");
        let stem = compose_filename_stem(&court, &date, &caseno, &serial);
        let re = grammar_re();
        prop_assert!(
            re.is_match(&stem),
            "grammar mismatch: stem={stem:?} re={:?}",
            re.as_str()
        );
    }

    #[test]
    fn stem_is_nfc(
        court in "[가-힣]{1,10}",
        yyyymmdd in "[0-9]{8}",
        caseno in "[가-힣0-9]{1,20}",
        serial in "[0-9]{1,7}",
    ) {
        let stem = compose_filename_stem(&court, &yyyymmdd, &caseno, &serial);
        let nfc: String = stem.nfc().collect();
        prop_assert_eq!(stem, nfc);
    }

    #[test]
    fn sanitize_never_emits_sep(
        caseno in "[가-힣0-9()]{0,60}",
    ) {
        let s = sanitize_case_number(&caseno);
        prop_assert!(
            !s.contains(SEP),
            "SEP `{SEP}` leaked into sanitize output {s:?} from {caseno:?}"
        );
    }
}
