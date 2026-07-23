#!/usr/bin/env bash
# Build a versioned host archive, CycloneDX SBOM, and SHA-256 checksums.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="${ROOT}/dist"
cd "$ROOT"

VERSION="$(
  awk '
    $0 == "[workspace.package]" { inside = 1; next }
    /^\[/ { inside = 0 }
    inside && /^version = / {
      gsub(/"/, "", $3)
      print $3
      exit
    }
  ' Cargo.toml
)"
[[ -n "$VERSION" ]] || { echo "unable to read workspace version" >&2; exit 1; }

HOST="$(rustc -vV | awk '/^host: / { print $2 }')"
[[ -n "$HOST" ]] || { echo "unable to determine Rust host triple" >&2; exit 1; }

if [[ "${ALLOW_DIRTY:-0}" != "1" ]] &&
   [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  echo "working tree is dirty; commit the reviewed release tree first" >&2
  echo "set ALLOW_DIRTY=1 only for a non-published development package" >&2
  exit 1
fi

SOURCE_SHA="$(git rev-parse HEAD)"
PACKAGE="marketfeed-v${VERSION}-${HOST}"
ARCHIVE="${PACKAGE}.tar.gz"
SBOM="marketfeed-v${VERSION}.cdx.json"
CARGO_PREFIX="$(cd "$(dirname "$(command -v cargo)")/.." && pwd)"
RUST_SYSROOT="$(rustc --print sysroot)"
SOURCE_DATE_EPOCH="$(git show -s --format=%ct HEAD)"
PACKAGE_RUSTFLAGS="${RUSTFLAGS:-}"
PACKAGE_RUSTFLAGS+=" --remap-path-prefix=${ROOT}=cryptofeed-rs"
PACKAGE_RUSTFLAGS+=" --remap-path-prefix=${CARGO_PREFIX}=cargo-home"
PACKAGE_RUSTFLAGS+=" --remap-path-prefix=${RUST_SYSROOT}=rust-sysroot"

mkdir -p "$DIST"
rm -f "${DIST}/${ARCHIVE}" "${DIST}/${SBOM}" "${DIST}/SHA256SUMS"

STAGE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/marketfeed-release.XXXXXX")"
cleanup() {
  rm -rf "$STAGE_ROOT"
}
trap cleanup EXIT

echo "building marketfeed ${VERSION} for ${HOST} from ${SOURCE_SHA}"
SOURCE_DATE_EPOCH="$SOURCE_DATE_EPOCH" RUSTFLAGS="$PACKAGE_RUSTFLAGS" \
  cargo build --release --locked -p marketfeed-daemon

VERSION_OUTPUT="$(target/release/marketfeed version)"
[[ "$VERSION_OUTPUT" == "marketfeed ${VERSION}" ]] || {
  echo "unexpected binary version: ${VERSION_OUTPUT}" >&2
  exit 1
}
target/release/marketfeed --help >/dev/null
if strings target/release/marketfeed |
   grep -E "/Users/[^/]+/|[A-Za-z]:\\\\\\\\Users\\\\\\\\"; then
  echo "release binary contains a workstation path" >&2
  exit 1
fi

./scripts/generate-sbom.sh --out-dir "${STAGE_ROOT}/sbom"
[[ -s "${STAGE_ROOT}/sbom/marketfeed.cdx.json" ]] ||
  { echo "canonical SBOM was not generated" >&2; exit 1; }
cp "${STAGE_ROOT}/sbom/marketfeed.cdx.json" "${DIST}/${SBOM}"

PACKAGE_DIR="${STAGE_ROOT}/${PACKAGE}"
mkdir -p "$PACKAGE_DIR"
cp target/release/marketfeed "$PACKAGE_DIR/"
cp LICENSE NOTICE README.md SECURITY.md "$PACKAGE_DIR/"

cat >"${PACKAGE_DIR}/BUILD-INFO.txt" <<EOF
project=cryptofeed-rs
maintainer=RSI Tech
website=https://rsitech.ai
contact=info@rsitech.ai
version=${VERSION}
source_repository=https://github.com/rsitech-ai/cryptofeed-rs
source_commit=${SOURCE_SHA}
target=${HOST}
rustc=$(rustc --version)
profile=release
license=Apache-2.0
EOF

COPYFILE_DISABLE=1 tar -C "$STAGE_ROOT" -czf "${DIST}/${ARCHIVE}" "$PACKAGE"

(
  cd "$DIST"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$ARCHIVE" "$SBOM" >SHA256SUMS
  else
    shasum -a 256 "$ARCHIVE" "$SBOM" >SHA256SUMS
  fi
)

echo "release package:"
echo "  ${DIST}/${ARCHIVE}"
echo "  ${DIST}/${SBOM}"
echo "  ${DIST}/SHA256SUMS"
