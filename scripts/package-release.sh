#!/usr/bin/env bash
# Build a versioned host archive, CycloneDX SBOM, and SHA-256 checksums.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="${DIST_DIR:-${ROOT}/dist}"
TARGET_DIR="${CARGO_TARGET_DIR:-${ROOT}/target}"
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
   [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then
  echo "working tree is dirty; commit the reviewed release tree first" >&2
  echo "set ALLOW_DIRTY=1 only for a non-published development package" >&2
  exit 1
fi

SOURCE_SHA="$(git rev-parse HEAD)"
PACKAGE="marketfeed-v${VERSION}-${HOST}"
ARCHIVE="${PACKAGE}.tar.gz"
SBOM="marketfeed-v${VERSION}-${HOST}.cdx.json"
CHECKSUMS="SHA256SUMS-${HOST}"
EVIDENCE="EVIDENCE-MANIFEST-${HOST}.txt"
CARGO_PREFIX="$(cd "$(dirname "$(command -v cargo)")/.." && pwd)"
RUST_SYSROOT="$(rustc --print sysroot)"
SOURCE_DATE_EPOCH="$(git show -s --format=%ct HEAD)"
PACKAGE_RUSTFLAGS="${RUSTFLAGS:-}"
PACKAGE_RUSTFLAGS+=" --remap-path-prefix=${ROOT}=cryptofeed-rs"
PACKAGE_RUSTFLAGS+=" --remap-path-prefix=${CARGO_PREFIX}=cargo-home"
PACKAGE_RUSTFLAGS+=" --remap-path-prefix=${RUST_SYSROOT}=rust-sysroot"
PACKAGE_RUSTFLAGS+=" --remap-path-prefix=${TARGET_DIR}=cargo-target"

mkdir -p "$DIST" "$TARGET_DIR"
rm -f \
  "${DIST}/${ARCHIVE}" \
  "${DIST}/${SBOM}" \
  "${DIST}/${CHECKSUMS}" \
  "${DIST}/${EVIDENCE}"

STAGE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/marketfeed-release.XXXXXX")"
cleanup() {
  rm -rf "$STAGE_ROOT"
}
trap cleanup EXIT

echo "building marketfeed ${VERSION} for ${HOST} from ${SOURCE_SHA}"
SOURCE_DATE_EPOCH="$SOURCE_DATE_EPOCH" \
  ZERO_AR_DATE=1 \
  RUSTFLAGS="$PACKAGE_RUSTFLAGS" \
  CARGO_TARGET_DIR="$TARGET_DIR" \
  cargo build --release --locked -p marketfeed-daemon

BINARY="${TARGET_DIR}/release/marketfeed"
case "$HOST" in
  *-apple-darwin)
    # The Apple linker emits a random LC_UUID. Normalize it after removing the
    # linker signature, then apply a stable ad hoc signature so the executable
    # remains launchable and independent builds are byte-identical.
    codesign --remove-signature "$BINARY"
    python3 ./scripts/normalize_macho.py "$BINARY" >/dev/null
    codesign --force --sign - --timestamp=none --identifier marketfeed "$BINARY"
    codesign --verify --strict "$BINARY"
    ;;
esac

VERSION_OUTPUT="$("$BINARY" version)"
[[ "$VERSION_OUTPUT" == "marketfeed ${VERSION}" ]] || {
  echo "unexpected binary version: ${VERSION_OUTPUT}" >&2
  exit 1
}
"$BINARY" --help >/dev/null
if strings "$BINARY" |
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
cp "$BINARY" "$PACKAGE_DIR/"
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

python3 ./scripts/create-reproducible-archive.py \
  --source-dir "$PACKAGE_DIR" \
  --archive "${DIST}/${ARCHIVE}" \
  --mtime "$SOURCE_DATE_EPOCH"

(
  cd "$DIST"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$ARCHIVE" "$SBOM" >"$CHECKSUMS"
  else
    shasum -a 256 "$ARCHIVE" "$SBOM" >"$CHECKSUMS"
  fi
)

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

SOURCE_INPUT_SHA256="$(
  git archive --format=tar HEAD |
    if command -v sha256sum >/dev/null 2>&1; then
      sha256sum
    else
      shasum -a 256
    fi |
    awk '{print $1}'
)"
cat >"${DIST}/${EVIDENCE}" <<EOF
schema=marketfeed-release-evidence-v1
project=cryptofeed-rs
version=${VERSION}
tag=${GITHUB_REF_NAME:-unreleased}
source_commit=${SOURCE_SHA}
source_date_epoch=${SOURCE_DATE_EPOCH}
source_archive_sha256=${SOURCE_INPUT_SHA256}
target=${HOST}
rustc=$(rustc --version)
cargo=$(cargo --version)
runner_os=${RUNNER_OS:-local}
runner_image=${ImageOS:-local}
runner_image_version=${ImageVersion:-unknown}
binary_sha256=$(sha256_file "$BINARY")
archive_sha256=$(sha256_file "${DIST}/${ARCHIVE}")
sbom_sha256=$(sha256_file "${DIST}/${SBOM}")
checksums_sha256=$(sha256_file "${DIST}/${CHECKSUMS}")
gate_release_build=pass
gate_binary_version_help=pass
gate_workstation_path_scan=pass
gate_platform_signature=pass_or_not_applicable
gate_sbom_generation=pass
gate_checksums_generation=pass
EOF

echo "release package:"
echo "  ${DIST}/${ARCHIVE}"
echo "  ${DIST}/${SBOM}"
echo "  ${DIST}/${CHECKSUMS}"
echo "  ${DIST}/${EVIDENCE}"
