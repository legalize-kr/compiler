//! Tiny helper that prints the Unicode DB version baked into `unicode-normalization`.
//!
//! Used by `legalize-pipeline/precedents/preflight_filename_audit.py` step (10) to
//! cross-check that the Python (`unicodedata.unidata_version`) and Rust sides agree.

fn main() {
    let v = unicode_normalization::UNICODE_VERSION;
    println!("{}.{}.{}", v.0, v.1, v.2);
}
