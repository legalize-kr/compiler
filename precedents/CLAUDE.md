# CLAUDE.md — compiler-for-precedent

`legalize-pipeline`의 `.cache/precedent/*.xml`을 입력으로 받아 bare Git 저장소(`precedent-kr`)를 직접 써내는 Rust 재구현 컴파일러입니다. 사용법·아키텍처 전반은 `README.md`를 참고하세요. 이 문서는 Claude Code 에이전트가 주의해야 할 규약을 기록합니다.

## 관련 저장소

| 저장소 | 관계 |
|---|---|
| `legalize-kr/legalize-pipeline` | 참조 구현 (Python). `precedents/converter.py`와 동일한 출력을 보장해야 함 |
| `legalize-kr/precedent-kr` | 출력 대상 저장소 (bare repo → clone) |

## Python ↔ Rust 동등성 (CRITICAL)

`precedents/converter.py`(파이썬)와 `src/render.rs`(러스트)는 동일한 입력 XML에 대해 **동일한 파일 경로·동일한 Markdown 본문**을 생성해야 합니다. 이 동등성이 깨지면 Python 파이프라인이 만든 기존 `precedent-kr` 히스토리와 Rust 컴파일러가 만드는 새 저장소가 어긋나, 웹사이트·배포 스냅샷·이력 diff가 무너집니다.

변경을 가할 때는 **양쪽을 같이 고칩니다**:

| 규약 | Rust 위치 | Python 위치 |
|---|---|---|
| 사건번호 sanitize | `render.rs:sanitize_case_number` | `precedents/converter.py:sanitize_case_number` |
| 파일명 byte cap | `render.rs:cap_filename_bytes` + `MAX_FILENAME_STEM_BYTES=180` | `precedents/converter.py:cap_filename_bytes` + `MAX_FILENAME_STEM_BYTES=180` |
| 경로 충돌 해소 | `render.rs:get_precedent_path` → `_{serial}.md` | `precedents/converter.py:get_precedent_path` → `_{serial}.md` |
| 캐시 iteration 순서 (충돌 시 clean path 승자 결정) | `main.rs` 에서 `entries.sort_by(\|l, r\| l.serial.cmp(&r.serial))` | `import_precedents.py` 에서 `sorted(PREC_CACHE_DIR.glob("*.xml"))` (Path lex 정렬) |
| 법원등급 매핑 | `render.rs:court_tier_label` | `precedents/converter.py:get_court_tier` |
| 사건종류 정규화 | `render.rs:normalize_case_type` | `precedents/converter.py:normalize_case_type` |
| 단기→서기 연도 | `xml_parser.rs` (단기 4200–4330 → `−2333`) | `precedents/converter.py:normalize_dangi_yyyymmdd` |

## 파일명 capping 규약

형사 병합/분리 판결은 하나의 판결 내 여러 연관 사건을 법원이 쉼표로 이어 단일 `사건번호` 필드에 기록하므로(예: `2011고합669, 743, ..., 985-1 (병합) (분리)`), 그대로 파일명으로 쓰면 macOS APFS `NAME_MAX=255 bytes` 제한을 넘어 `git checkout`이 실패합니다.

- **임계값**: 파일명 stem(`.md` 제외)을 UTF-8 기준 **180바이트**로 cap.
- **truncation 규약**: UTF-8 문자 경계에서 자른 뒤 `_{판례일련번호}`를 접미사로 붙여 고유성과 역추적성을 동시에 확보.
- **재현성**: Rust와 Python 양쪽이 정확히 같은 바이트 임계값·같은 UTF-8 경계 규약을 사용해야 양쪽 출력이 bit-identical.
- **업스트림 잘림**: API가 긴 나열을 `....`로 잘라 보내는 케이스(예: `..._3461 ....md`)는 현재 파이프라인이 별도 정리하지 않고 그대로 포함합니다. 정리하려면 양쪽을 동시에 수정.

## 개발

```bash
cargo test      # 단위·통합 테스트 (end-to-end bare repo clone 검증 포함)
cargo fmt
cargo clippy
cargo build --release
```

테스트 추가 시 원칙: **경로 생성·Markdown 본문·공포일자/선고일자 clamp**처럼 Python과의 동등성이 깨지기 쉬운 지점은 반드시 회귀 테스트를 둡니다.

## 커밋 규약

- 커밋 author/committer: `legalize-kr-bot <bot@legalize.kr>` (출력 저장소와 일치)
- 소스 변경 커밋: 일반 개발자 author (이 저장소는 bot이 push하지 않음)
- 단일 로직 변경이 Python 재구현과 연동되면 commit message에 sibling 저장소 경로를 명시해 추적성을 남깁니다.
