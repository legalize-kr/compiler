//! Property tests for `compose_filename_stem` (mirrors Python Hypothesis suite).
//!
//! Invariants:
//!   - NFC idempotence: composing twice (input already NFC) yields the same stem.
//!   - Determinism: same input → same output.
//!   - Grammar regex: stem matches `^[^/_]+_\d{4}-\d{2}-\d{2}_.+$` (left-anchored).
//!     CASENO may contain `_` (merged cases such as `2000므1257_본소_1264_반소`),
//!     but court name has no `_` and date is fixed `YYYY-MM-DD`, so left-anchored
//!     parsing with `splitn(3, SEP)` always isolates (court, date, caseno).

use precedent_kr_compiler::render::{SEP, compose_filename_stem, sanitize_case_number};
use proptest::prelude::*;
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

fn grammar_re() -> Regex {
    // Left-anchored: court has no SEP; CASENO may contain SEP. The date sits between
    // the first two SEPs, so the first two splits always isolate court+date.
    let escaped = regex::escape(SEP);
    Regex::new(&format!(
        "^[^/{escaped}]+{escaped}\\d{{4}}-\\d{{2}}-\\d{{2}}{escaped}.+$"
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
    fn sanitize_is_deterministic_and_nfc(
        caseno in "[가-힣0-9(),]{0,60}",
    ) {
        let a = sanitize_case_number(&caseno);
        let b = sanitize_case_number(&caseno);
        prop_assert_eq!(&a, &b);
        let nfc: String = a.nfc().collect();
        prop_assert_eq!(a, nfc);
    }
}
