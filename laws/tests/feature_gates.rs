//! Pins the default feature wiring so the perf-path code in `main.rs` and
//! `git_repo.rs` (`mimalloc` allocator, `libdeflater` compressor) is actually
//! compiled in for the default `cargo build` / `cargo test`. A regression
//! where these gates silently fall back to the system allocator / `zlib-rs`
//! caused observable performance loss without any test signal — see PR #5.

#[test]
fn default_build_enables_mimalloc_allocator() {
    assert!(
        cfg!(feature = "mimalloc"),
        "default cargo build must enable the `mimalloc` feature; \
         otherwise the global allocator falls back to the system allocator"
    );
}

#[test]
fn default_build_enables_libdeflater_compressor() {
    assert!(
        cfg!(feature = "libdeflater"),
        "default cargo build must enable the `libdeflater` feature; \
         otherwise pack compression silently falls back to `zlib-rs`"
    );
}
