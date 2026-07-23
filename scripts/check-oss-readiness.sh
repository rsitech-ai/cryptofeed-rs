#!/usr/bin/env bash
# Fail-closed checks for the public repository surface.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

fail() {
  echo "oss-readiness: $*" >&2
  exit 1
}

existing_tracked_files() {
  while IFS= read -r path; do
    [[ -e "$path" ]] && printf '%s\n' "$path"
  done < <(git ls-files)
}

required=(
  LICENSE
  NOTICE
  README.md
  SECURITY.md
  CONTRIBUTING.md
  CODE_OF_CONDUCT.md
  GOVERNANCE.md
  SUPPORT.md
  RELEASING.md
  CODEOWNERS
)

for path in "${required[@]}"; do
  [[ -s "$path" ]] || fail "missing or empty required file: $path"
done

[[ ! -e LICENSE-MIT ]] || fail "MIT license file remains"
[[ ! -e LICENSE-APACHE ]] || fail "legacy Apache license filename remains"

grep -Fq 'license = "Apache-2.0"' Cargo.toml ||
  fail "workspace license is not Apache-2.0"
grep -Fq 'repository = "https://github.com/rsitech-ai/cryptofeed-rs"' Cargo.toml ||
  fail "workspace repository is not the RSI Tech organization repository"
grep -Fq 'homepage = "https://rsitech.ai"' Cargo.toml ||
  fail "workspace homepage is not RSI Tech"
grep -Fq 'info@rsitech.ai' SECURITY.md ||
  fail "confidential project contact is missing"

if git grep -I -n -E \
  'github\.com/s1korrrr/cryptofeed-rs|mrsikorarafal@gmail\.com|Apache-2\.0 OR MIT|LICENSE-MIT|LICENSE-APACHE|dual-licensed|dual licensed' \
  -- . ':(exclude)scripts/check-oss-readiness.sh'; then
  fail "stale personal or dual-license reference found"
fi

if existing_tracked_files |
   grep -E '(^|/)(\.DS_Store|\.cursor|\.codex|\.superpowers|\.vscode)(/|$)'; then
  fail "tracked workstation or agent metadata found"
fi

if existing_tracked_files | grep -E \
  '^docs/ops/(canary_evidence|private_canary_evidence|soak_evidence|soak_evidence_w3)(/|$)'; then
  fail "raw operator evidence is tracked"
fi

if git grep -I -n -E '/Users/[^/]+/' -- \
  . \
  ':(exclude)scripts/check-oss-readiness.sh' \
  ':(exclude)scripts/package-release.sh' ||
   git grep -I -n -F ":\\\\Users\\\\" -- \
  . \
  ':(exclude)scripts/check-oss-readiness.sh' \
  ':(exclude)scripts/package-release.sh'; then
  fail "workstation path found in tracked content"
fi

if command -v gitleaks >/dev/null 2>&1; then
  gitleaks dir . --redact --no-banner
else
  echo "oss-readiness: gitleaks unavailable; secret scan skipped" >&2
fi

echo "oss-readiness: PASS"
