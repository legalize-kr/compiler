# precedent-kr-compiler

[legalize-kr/legalize-pipeline]으로 만들어진 `.cache/precedent` 디렉토리를 git으로 바꿔주는
컴파일러입니다. 이 프로그램은 법제처 API를 직접 호출하지 않고, 이미 존재하는
캐시만 입력으로 받습니다.

[legalize-kr/legalize-pipeline]: https://github.com/legalize-kr/legalize-pipeline

## 사용법

```bash
precedent-kr-compiler <input_cache_dir> [-o <output_git_dir>]
```

기본 출력 경로는 `./output.git`입니다. 결과물은 bare repo이므로 내용을 보려면
clone해서 확인하면 됩니다.

```bash
precedent-kr-compiler ../.cache/precedent
git clone ./output.git ./precedent-kr
cd precedent-kr
```

출력 bare repo 경로를 직접 지정할 수도 있습니다.

```bash
precedent-kr-compiler ../.cache/precedent -o ./another.git
```

## 동작 방식

2-pass로 동작합니다.

1. `{cache_dir}/*.xml`의 메타데이터만 읽어 정렬용 entry를 만듭니다.
   - `<PrecService>` 루트가 아니거나 `판례정보일련번호`가 없는 파일은 warning과 함께 건너뜁니다.
2. entry를 다음 순서로 정렬합니다.
   - `선고일자 asc` (빈 날짜는 마지막)
   - `사건번호 asc`
   - `판례일련번호 asc (numeric)`
3. 경로 충돌 규칙을 적용해 출력 파일 경로를 확정합니다.
   - 기본 경로: `{사건종류}/{법원등급}/{사건번호}.md`
   - 충돌 시: `{사건종류}/{법원등급}/{사건번호}_{판례일련번호}.md`
4. 정렬된 순서대로 XML 본문을 다시 파싱해 Markdown과 commit message를 만들고 commit을 작성합니다.
   - 이 단계는 chunk 단위로 병렬 render를 수행하면서, main thread는 순서대로 commit만 씁니다.

## 출력 특성

- 매 실행마다 fresh bare repo를 새로 만듭니다.
- branch는 `main`입니다.
- object database는 direct pack writer로 만들고, 마지막에 `.idx` v2 index로 마무리합니다.
- refs backend는 `HEAD`와 `refs/heads/main` loose ref 파일을 직접 씁니다.
- commit author/committer는 `legalize-kr-bot <bot@legalize.kr>`입니다.
- commit timestamp는 선고일자 기준 KST `12:00:00`입니다.
- `1970-01-01` 이전 날짜 및 빈 선고일자는 epoch 이전 commit을 피하기 위해 clamp합니다.

## 출력 저장소 구조

```
{사건종류}/
  {법원등급}/
    {사건번호}.md
```

예시:
- `민사/대법원/2024다12345.md`
- `형사/하급심/2023고합678.md`
- `일반행정/대법원/2022두9012.md`

`사건종류`: 민사, 형사, 일반행정, 세무, 특허, 가사, 기타  
`법원등급`: 대법원 (법원종류코드 `400201`), 하급심 (`400202`), 미분류 (기타)

## 개발

```bash
# test
cargo test

# format
cargo fmt

# lint
cargo clippy

# release build
cargo build --release
```

### 크로스 컴파일 방법

macOS에서:

```bash
brew install filosottile/musl-cross/musl-cross

rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl

cargo build -r --target x86_64-unknown-linux-musl
cargo build -r --target aarch64-unknown-linux-musl
```

&nbsp;

---

*precedent-kr-compiler* is primarily distributed under the terms of both the
[Apache License (Version 2.0)] and the [MIT license].

[MIT license]: LICENSE-MIT
[Apache License (Version 2.0)]: LICENSE-APACHE
