# legalize-kr-compiler

[legalize-kr/legalize-pipeline]으로 만들어진 `.cache` 디렉토리를 git으로 바꿔주는
컴파일러입니다. 이 프로그램은 법제처 API를 직접 호출하지 않고, 이미 존재하는
캐시만 입력으로 받습니다. API 응답 캐시는 [여기]에서 다운받으실 수 있습니다.

[legalize-kr/legalize-pipeline]: https://github.com/legalize-kr/legalize-pipeline
[여기]: https://github.com/legalize-kr/legalize-kr/discussions/8

## 사용법
```bash
legalize-kr-compiler <input_cache_dir> [-o <output_git_dir>]
```

기본 출력 경로는 `./output.git`입니다. 결과물은 bare repo이므로 내용을 보려면
clone해서 확인하면 됩니다.

```
legalize-kr-compiler ../.cache
git clone ./output.git ./legalize-kr
cd legalize-kr
```

출력 bare repo 경로를 직접 지정할 수도 있습니다.

```bash
legalize-kr-compiler ../.cache -o ./another.git
```

## 동작 방식

2-pass로 동작합니다.

1. `history/*.json`에서 `MST -> 제개정구분명` 매핑을 로드합니다.
   - 이때, `history/`가 없으면 amendment 정보 없이 `detail/`만으로 빌드합니다.
2. `detail/*.xml`의 메타데이터만 읽어 정렬용 entry를 만듭니다.
   - XML이 아니라 HTML error page처럼 파싱 불가능한 detail 파일은 warning과 함께 건너뜁니다.
3. entry를 다음 순서로 정렬합니다.
   - `공포일자 asc`
   - `법령명 asc`
   - `공포번호 asc (numeric)`
   - `MST asc (numeric)`
4. 경로 충돌 규칙을 적용해 출력 파일 경로를 확정합니다.
5. 정렬된 순서대로 XML 본문만 다시 파싱해 Markdown과 commit message를 만들고 commit을 작성합니다.
   - 이 단계는 chunk 단위로 병렬 render를 수행하면서, main thread는 순서대로 commit만 씁니다.

## 출력 특성

- 매 실행마다 fresh bare repo를 새로 만듭니다.
- branch는 `main`입니다.
- object database는 direct pack writer로 만들고, 마지막에 `git index-pack`으로 마무리합니다.
- refs backend는 `HEAD`와 `refs/heads/main` loose ref 파일을 직접 씁니다.
- commit author/committer는 `legalize-kr-bot <bot@legalize.kr>`입니다.
- commit timestamp는 공포일자 기준 KST `12:00:00`입니다.
- `1970-01-01` 이전 날짜는 epoch 이전 commit을 피하기 위해 clamp합니다.

## 개발
```bash
# test
cargo test

# format
cargo fmt

# lint
cargo clippy
```

## 프로파일링
`profiling` profile은 `release` 최적화를 유지하면서 debug symbols를 포함합니다.

```bash
cargo build --profile profiling
samply record -- ./target/profiling/legalize-kr-compiler ../.cache
```

## Cross-compiler on macOS
```bash
# Install musl toolchain
brew install filosottile/musl-cross/musl-cross

# Build against x86_64
cargo build -r --target x86_64-unknown-linux-musl
# Build against aarch64
cargo build -r --target aarch64-unknown-linux-musl
```

&nbsp;

---

*legalize-kr-compiler* is primarily distributed under the terms of both the
[Apache License (Version 2.0)] and the [MIT license]. See [COPYRIGHT] for
details.

[MIT license]: LICENSE-MIT
[Apache License (Version 2.0)]: LICENSE-APACHE
[COPYRIGHT]: COPYRIGHT
