//! Library facade so integration tests, benches, and helper bins (e.g. `tests/oracle.rs`,
//! `src/bin/print_unicode_version.rs`) can import the compiler internals without
//! re-declaring the same modules. The `main.rs` binary uses the same modules.

pub mod render;
pub mod xml_parser;
