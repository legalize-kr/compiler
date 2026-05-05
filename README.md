# legalize-kr / compiler

[legalize-kr/legalize-pipeline]이 만드는 `.cache/` 디렉토리를 입력으로 받아
법령(`legalize-kr`), 판례(`precedent-kr`), 행정규칙(`admrule-kr`),
자치법규(`ordinance-kr`) bare Git 저장소를 직접 써내는 Rust 컴파일러
모음입니다. 각 도메인이 한 Cargo workspace 안에 멤버 크레이트로
공존합니다.

[legalize-kr/legalize-pipeline]: https://github.com/legalize-kr/legalize-pipeline

## Workspace 구조

```
compiler/
├── Cargo.toml         # virtual workspace
├── Cargo.lock         # 단일 lockfile
├── admrules/          # 행정규칙 컴파일러 (binary: admrule-kr-compiler)
├── laws/              # 법령 컴파일러 (binary: legalize-kr-compiler)
├── ordinances/        # 자치법규 컴파일러 (binary: ordinance-kr-compiler)
└── precedents/        # 판례 컴파일러 (binary: precedent-kr-compiler)
```

멤버별 사용 문서와 생성 저장소 README 원본은 다음 위치에 있습니다:

- 법령 컴파일러: [`laws/README.md`](laws/README.md)
- 판례 컴파일러: [`precedents/README.md`](precedents/README.md)
- 행정규칙 결과 README: [`admrules/assets/README.md`](admrules/assets/README.md)
- 자치법규 결과 README: [`ordinances/assets/README.md`](ordinances/assets/README.md)

## 빠른 시작

```bash
# 워크스페이스 전체 빌드 (네 컴파일러 바이너리 동시 산출)
cargo build --workspace --release

# 법령 컴파일
./target/release/legalize-kr-compiler ../.cache -o ./output.git

# 판례 컴파일
./target/release/precedent-kr-compiler ../.cache/precedent -o ./precedent-output.git

# 행정규칙 컴파일
./target/release/admrule-kr-compiler ../.cache/admrule -o ./admrule-output.git --bare

# 자치법규 컴파일
./target/release/ordinance-kr-compiler ../.cache/ordinance -o ./ordinance-output.git --bare
```

## CI 4종 게이트

push 전 다음 4종을 모두 로컬에서 통과시키세요:

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

---

*legalize-kr / compiler* is primarily distributed under the terms of both the
[Apache License (Version 2.0)] and the [MIT license]. See [COPYRIGHT] for
details.

[MIT license]: LICENSE-MIT
[Apache License (Version 2.0)]: LICENSE-APACHE
[COPYRIGHT]: COPYRIGHT
