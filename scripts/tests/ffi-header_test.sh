#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-${ROOT}/target}"
TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/marketfeed-ffi-header.XXXXXX")"

# shellcheck disable=SC2329 # Invoked indirectly by the EXIT trap below.
cleanup() {
  rm -rf "${TEST_DIR}"
}
trap cleanup EXIT

cc \
  -std=c11 \
  -Wall \
  -Wextra \
  -Werror \
  -I "${ROOT}/crates/ffi/include" \
  -fsyntax-only \
  "${ROOT}/crates/ffi/tests/header_smoke.c"

CARGO_TARGET_DIR="${TARGET_DIR}" cargo build --locked -p marketfeed-ffi

case "$(uname -s)" in
  Darwin)
    LIBRARY="${TARGET_DIR}/debug/libmarketfeed_ffi.dylib"
    ;;
  Linux)
    LIBRARY="${TARGET_DIR}/debug/libmarketfeed_ffi.so"
    ;;
  *)
    printf 'unsupported ffi header test host: %s\n' "$(uname -s)" >&2
    exit 1
    ;;
esac

test -s "${LIBRARY}"
SYMBOLS="$(nm -g "${LIBRARY}")"
for symbol in marketfeed_version marketfeed_fixed_parse marketfeed_fixed_parse_cstr; do
  grep -Eq "[[:space:]_]${symbol}$" <<<"${SYMBOLS}" || {
    printf 'missing exported ABI symbol: %s\n' "${symbol}" >&2
    exit 1
  }
done

cc \
  -std=c11 \
  -Wall \
  -Wextra \
  -Werror \
  -I "${ROOT}/crates/ffi/include" \
  "${ROOT}/crates/ffi/tests/header_smoke.c" \
  -L "${TARGET_DIR}/debug" \
  -lmarketfeed_ffi \
  -Wl,-rpath,"${TARGET_DIR}/debug" \
  -o "${TEST_DIR}/header_smoke"
"${TEST_DIR}/header_smoke"

printf 'ffi-header-test: PASS\n'
