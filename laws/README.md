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

   이 4-튜플 순서는 [legalize-pipeline]의 `laws.converter.entry_sort_key`와
   동일하게 맞춰져 있습니다. Python 재구현이 동일한 canonical 파일을 고르도록
   하려면 양쪽을 같이 바꿔야 합니다. 직접 영향을 받는 Python 호출 지점:
   `laws/rebuild.py`, `laws/import_laws.py` (API/cache/CSV 모드),
   `laws/update.py`.
4. 경로 충돌 규칙을 적용해 출력 파일 경로를 확정합니다.
   - 같은 structural path를 두 개 이상의 `법령ID`가 공유할 때만 qualified
     suffix(`법률(법률).md`)가 붙습니다.
   - 정렬 결과 **먼저 오는 entry가 canonical(`법률.md`)**이 되는 first-write-wins.
     이 때문에 (3)의 4-튜플 정렬이 canonical 경로 선택의 tiebreaker가 됩니다.
     정렬 키가 달라지면 canonical/qualified 배정이 뒤집혀 git history의
     `법률.md` 연혁이 끊길 수 있으므로, Python과 Rust의 정렬 키는
     반드시 일치해야 합니다.

[legalize-pipeline]: https://github.com/legalize-kr/legalize-pipeline
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

# profiling
cargo build --profile profiling
samply record -- target/profiling/legalize-kr-compiler .cache
```

### How to cross-compile
On macOS:
```bash
# Install musl toolchain
brew install filosottile/musl-cross/musl-cross

rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl x86_64-apple-darwin aarch64-apple-darwin

cargo build -r --target x86_64-unknown-linux-musl
cargo build -r --target aarch64-unknown-linux-musl
cargo build -r --target aarch64-apple-darwin
cargo build -r --target x86_64-apple-darwin

mkdir -p target/universal2-apple-darwin/release
lipo -create -output target/{universal2,aarch64,x86_64}-apple-darwin/release/legalize-kr-compiler
```

On Linux:
```bash
# Install zig, rustup, and cargo-zigbuild
# using the method appropriate for your Linux distribution.

rustup target add \
  x86_64-unknown-linux-musl \
  aarch64-unknown-linux-musl \
  arm-unknown-linux-musleabi \
  arm-unknown-linux-musleabihf \
  armv7-unknown-linux-musleabi \
  armv7-unknown-linux-musleabihf \
  i586-unknown-linux-musl \
  i686-unknown-linux-musl \
  loongarch64-unknown-linux-musl \
  powerpc64le-unknown-linux-musl \
  riscv64gc-unknown-linux-musl \
  x86_64-apple-darwin \
  aarch64-apple-darwin

cargo zigbuild --no-default-features -r --target x86_64-unknown-linux-musl
cargo zigbuild --no-default-features -r --target aarch64-unknown-linux-musl
cargo zigbuild --no-default-features -r --target arm-unknown-linux-musleabi
cargo zigbuild --no-default-features -r --target arm-unknown-linux-musleabihf
cargo zigbuild --no-default-features -r --target armv7-unknown-linux-musleabi
cargo zigbuild --no-default-features -r --target armv7-unknown-linux-musleabihf
cargo zigbuild --no-default-features -r --target i586-unknown-linux-musl
cargo zigbuild --no-default-features -r --target i686-unknown-linux-musl
cargo zigbuild --no-default-features -r --target loongarch64-unknown-linux-musl
cargo zigbuild --no-default-features -r --target powerpc64le-unknown-linux-musl
cargo zigbuild --no-default-features -r --target riscv64gc-unknown-linux-musl
cargo zigbuild --no-default-features -r --target universal2-apple-darwin
```

&nbsp;

---

*legalize-kr-compiler* is primarily distributed under the terms of both the
[Apache License (Version 2.0)] and the [MIT license]. See [COPYRIGHT] for
details.

[MIT license]: LICENSE-MIT
[Apache License (Version 2.0)]: LICENSE-APACHE
[COPYRIGHT]: COPYRIGHT
