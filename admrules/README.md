# admrule-kr-compiler

[legalize-kr/legalize-pipeline]으로 만들어진 `.cache/admrule` 연혁 XML 캐시를
git history DB로 바꿔주는 컴파일러입니다. 이 프로그램은 국가법령정보센터 API를
직접 호출하지 않고, 이미 존재하는 캐시만 입력으로 받습니다.

[legalize-kr/legalize-pipeline]: https://github.com/legalize-kr/legalize-pipeline

## 사용법

```bash
admrule-kr-compiler <input_cache_dir> [-o <output_git_dir>] [--validate] [--manifest <path>]
```

기본 출력 경로는 `./output.git`입니다. 결과물은 bare repo이므로 내용을 보려면
clone해서 확인하면 됩니다.

```bash
admrule-kr-compiler ../.cache/admrule
git clone ./output.git ./admrule-kr
cd admrule-kr
```

출력 bare repo 경로를 직접 지정할 수도 있습니다.

```bash
admrule-kr-compiler ../.cache/admrule -o ./another.git
```

저장소를 쓰기 전에 캐시 상태만 JSON으로 확인하려면 `--validate`를 사용합니다.
빌드 결과의 `HEAD`와 엔트리 수는 `--manifest <path>`로 기록할 수 있습니다.
기존 Markdown tree 디렉토리 출력이 필요하면 `--tree`를 사용합니다.

```bash
admrule-kr-compiler ../.cache/admrule --tree -o ./admrule-tree
```

## 동작 방식

2-pass로 동작합니다.

1. `{cache_dir}/*.xml`의 행정규칙 메타데이터와 본문을 읽어 revision entry를 만듭니다.
2. 원천의 `상위부처명`, `소관부처명`, `담당부서기관명`을 정규화해 저장소
   기관 경로를 결정합니다.
3. 경로 충돌 규칙을 적용해 출력 파일 경로를 확정합니다.
   - 기본 경로: `{기관경로...}/{행정규칙종류}/{행정규칙명}/본문.md`
   - 충돌 시: 행정규칙명에 `발령번호`, `행정규칙일련번호` 또는 두 값을 조합한
     접미사를 붙입니다.
4. 같은 행정규칙의 이력 identity는 `행정규칙ID`입니다. 값이 없을 때만
   `행정규칙일련번호`로 fallback합니다.
5. entry를 다음 순서로 정렬합니다.
   - `발령일자 asc`
   - `행정규칙일련번호 asc (numeric)`
   - `출력 경로 asc`
6. 정렬된 순서대로 Markdown과 commit message를 만들고 commit을 작성합니다.
   같은 identity의 개정으로 경로가 바뀌면 이전 경로의 파일을 함께 삭제합니다.
   폐지 코드 `200404`, `200410`은 해당 identity의 최신 경로를 삭제합니다.
   `폐지제정`(`200407`)은 새 본문을 남깁니다.
7. 전체 빌드에서 `current_snapshot.json`이 있으면 원천 API가 현행으로 선택한
   판본을 최종 HEAD에 복원합니다. 시행 예정판도 발령일순 이력에 보존합니다.
   복원 또는 제외 커밋은 목록 관측일을 사용합니다.
   필요한 상세 XML이 없거나 ID가 맞지 않으면 빌드를 중단합니다.

`current_snapshot.json`은 `admrules.fetch_cache`가 만드는 캐시 계약입니다.
`schema_version: 1`, 관측일 `observed_on`, ID별 일련번호 `rules`를 포함합니다.
목록에는 없지만 상세 API가 현행으로 확인한 자료는 `supplemental`에 근거를 남기고 보존합니다.
목록에서 사라진 과거 XML은 `retired/`에 보관하며 컴파일 대상에 포함합니다.
동일 일련번호가 두 위치에 있으면 활성 캐시를 우선합니다.
YAML 문자열은 길이 때문에 줄을 나누지 않습니다. 줄바꿈과 제어 문자는
큰따옴표 문자열 안에서 이스케이프하며, Python과 같은 값과 바이트를 보존합니다.
이 파일이 없는 옛 캐시는 기존 상태 판정을 사용하므로, 전체 재생성 전에
파이프라인으로 캐시를 갱신해야 합니다. `--limit` 표본 빌드는 전체 현행 선택을 적용하지 않습니다.

## 출력 특성

- 매 실행마다 fresh bare repo를 새로 만듭니다.
- branch는 `main`입니다.
- `HEAD`는 현행 스냅샷입니다. 폐지 전 본문은 Git history에 남습니다.
- 결과 저장소의 루트 `README.md`는 [`assets/README.md`](assets/README.md)를 사용합니다.
- commit author/committer는 `legalize-kr-bot <bot@legalize.kr>`입니다.
- commit timestamp는 발령일자 기준 KST `12:00:00`입니다.
- `1970-01-01` 이전 날짜 또는 잘못된 날짜는 epoch 이전 commit을 피하기 위해 clamp합니다.

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

&nbsp;

---

*admrule-kr-compiler* is primarily distributed under the terms of both the
[Apache License (Version 2.0)] and the [MIT license].

[MIT license]: ../LICENSE-MIT
[Apache License (Version 2.0)]: ../LICENSE-APACHE
