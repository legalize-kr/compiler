# legalize-kr / compiler

[legalize-kr/legalize-pipeline]이 만드는 `.cache/` 디렉토리를 입력으로 받아
법령(`legalize-kr`) 및 판례(`precedent-kr`) bare Git 저장소를 직접 써내는
Rust 컴파일러 모음입니다. 두 도메인이 한 Cargo workspace 안에 멤버 크레이트로
공존합니다.

[legalize-kr/legalize-pipeline]: https://github.com/legalize-kr/legalize-pipeline

## Workspace 구조

```
compiler/
├── Cargo.toml         # virtual workspace
├── Cargo.lock         # 단일 lockfile
├── laws/              # 법령 컴파일러 (binary: legalize-kr-compiler)
└── precedents/        # 판례 컴파일러 (binary: precedent-kr-compiler)
```

각 멤버는 독립 패키지이며 자체 README/CLAUDE.md를 가집니다:

- 법령: [`laws/README.md`](laws/README.md)
- 판례: [`precedents/README.md`](precedents/README.md), [`precedents/CLAUDE.md`](precedents/CLAUDE.md) (Python ↔ Rust 동등성 규약)

## 빠른 시작

```bash
# 워크스페이스 전체 빌드 (두 바이너리 동시 산출)
cargo build --workspace --release

# 법령 컴파일
./target/release/legalize-kr-compiler ../.cache -o ./output.git

# 판례 컴파일
./target/release/precedent-kr-compiler ../.cache/precedent -o ./precedent-output.git
```

## CI 4종 게이트

`legalize-kr` 워크스페이스 CLAUDE.md 규약에 따라 push 전 다음 4종을 모두
로컬에서 통과시키세요:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --no-deps -- -D warnings
cargo shear
cargo test --workspace
```

## pre-commit 훅 활성화

`.githooks/pre-commit`에 `cargo fmt --check` 게이트가 있습니다. clone 또는
worktree 직후 한 번 실행해 활성화하세요:

```bash
git config core.hooksPath .githooks
```

## 통합 이력

본 저장소는 2026-04-26에 `9bow/compiler-for-precedent`(판례 컴파일러)를
`git filter-repo --to-subdirectory-filter precedents`로 경로 재작성한 뒤
`git merge --allow-unrelated-histories`로 흡수해 통합되었습니다.
원본 commit이 끊김 없이 보존되어 `git log --follow precedents/src/render.rs`가
정상 동작합니다.
