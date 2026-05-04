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
   - 먼저 `판례일련번호 asc` 문자열 순서로 정렬해 경로 충돌 시 clean path 승자를 결정합니다.
   - 이후 commit 순서는 `선고일자 asc`, `판례일련번호 asc` 문자열 순서입니다.
   - 빈 `선고일자`는 Python `import_precedents --git`처럼 마지막에 배치합니다.
   - `0000`/`0001` 계열 sentinel 날짜는 원본 문자열 순서대로 앞쪽에 배치되고, Git timestamp는 epoch로 clamp됩니다.
3. 합성 파일명 규칙과 경로 충돌 규칙을 적용해 출력 파일 경로를 확정합니다.
   - 기본 경로: `{사건종류}/{법원등급}/{법원명}_{선고일자}_{사건번호}.md`
   - 충돌 시: `{사건종류}/{법원등급}/{법원명}_{선고일자}_{사건번호}_{판례일련번호}.md`
4. 확정된 순서대로 XML 본문을 다시 파싱해 Markdown과 commit message를 만들고 commit을 작성합니다.
   - 이 단계는 chunk 단위로 병렬 render를 수행하면서, main thread는 순서대로 commit만 씁니다.

## 출력 특성

- 매 실행마다 fresh bare repo를 새로 만듭니다.
- branch는 `main`입니다.
- object database는 direct pack writer로 만들고, 마지막에 `.idx` v2 index로 마무리합니다.
- refs backend는 `HEAD`와 `refs/heads/main` loose ref 파일을 직접 씁니다.
- commit author/committer는 `legalize-kr-bot <bot@legalize.kr>`입니다.
- commit timestamp는 선고일자 기준 KST `12:00:00`입니다.
- `1970-01-01` 이전 날짜 및 빈 선고일자는 epoch 이전 commit을 피하기 위해 clamp합니다.
- 업스트림이 오래된 판례의 `선고일자`를 단기(檀紀) 4자리 연도로 반환하는 경우가 있습니다(예: `42890525`). 파싱 시점에 단기 범위(4200–4330)를 서기로 변환(`CE = 단기 − 2333`)하여 정렬·타임스탬프·frontmatter가 모두 서기 기준으로 일치하도록 처리합니다. 대상 판례 목록은 생성된 저장소 `README.md`를 참고하세요.
- **긴 파일명 capping (`NAME_MAX=255` 대응)**: 형사 병합/분리 판결은 `사건번호` 한 필드에 수십~수백 개의 사건번호를 쉼표로 나열하는 경우가 있습니다(예: `2011고합669, 743, 746, ..., 985-1 (병합) (분리)`). 그대로 파일명으로 쓰면 macOS APFS의 `NAME_MAX=255 bytes` 제한을 초과해 `git clone` 후 `checkout`이 실패합니다. `render.rs:cap_caseno_slot`이 파일명 stem(확장자 제외)을 UTF-8 기준 180바이트로 cap하고, 법원명/선고일자 slot은 보존한 채 사건번호 slot만 줄입니다. 잘린 경우 `_{판례일련번호}`를 접미사로 붙여 고유성과 역추적성을 보존합니다. 업스트림 API가 사건번호 끝을 `....`로 잘라 보내는 경우가 있는데, 현재는 그대로 포함되므로 잘림 흔적이 파일명에 남을 수 있습니다.
- `--emit-legacy-paths <path>`를 지정하면 기존 단일-key 파일명에서 새 합성 파일명으로의 mapping JSON을 함께 씁니다. `--legacy-precedent-root <path>`를 같이 넘기면 실제 기존 파일이 없는 항목은 `old_path: null`로 기록합니다.

## 출력 저장소 구조

```
{사건종류}/
  {법원등급}/
    {법원명}_{선고일자}_{사건번호}.md
```

예시:
- `민사/대법원/대법원_2024-01-01_2024다12345.md`
- `형사/하급심/서울중앙지방법원_2023-02-03_2023고합678.md`
- `일반행정/대법원/대법원_2022-12-31_2022두9012.md`

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

[MIT license]: ../LICENSE-MIT
[Apache License (Version 2.0)]: ../LICENSE-APACHE
