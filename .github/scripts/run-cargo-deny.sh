#!/usr/bin/env bash
set -euo pipefail

registry=".github/dependency-update-lifecycle.json"
target="x86_64-unknown-linux-musl"
test -f "$registry"
: "${RUNNER_TEMP:?GitHub runner temporary directory is required}"

mapfile -t metadata < <(python3 - "$registry" "$target" <<'PY'
import json
import re
import sys

registry_path, target = sys.argv[1:]
data = json.load(open(registry_path, encoding="utf-8"))
entry = next((item for item in data["entries"] if item["id"] == "cargo-deny"), None)
if entry is None:
    raise SystemExit("missing cargo-deny lifecycle entry")
pin = entry["pin"]
artifact = pin["artifacts"].get(target)
if artifact is None:
    raise SystemExit(f"unsupported cargo-deny target: {target}")
digest = artifact["sha256"]
if not re.fullmatch(r"[0-9a-f]{64}", digest):
    raise SystemExit("invalid cargo-deny SHA-256")
print(pin["repository"])
print(pin["version"])
print(digest)
PY
)

test "${#metadata[@]}" -eq 3
repository="${metadata[0]}"
version="${metadata[1]}"
expected_sha256="${metadata[2]}"

test "$RUNNER_OS" = Linux
test "$RUNNER_ARCH" = X64
test "$(uname -m)" = x86_64

work_dir="$(mktemp -d "$RUNNER_TEMP/cargo-deny.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT
archive="$work_dir/cargo-deny.tar.gz"

curl --proto '=https' --tlsv1.2 --fail --location --retry 3 --retry-delay 2 \
  "https://github.com/${repository}/releases/download/${version}/cargo-deny-${version}-${target}.tar.gz" \
  --output "$archive"
printf '%s  %s\n' "$expected_sha256" "$archive" | sha256sum --check --strict

mkdir -p "$work_dir/bin"
tar -xzf "$archive" -C "$work_dir/bin" --strip-components=1
binary="$work_dir/bin/cargo-deny"
test -x "$binary"
test "$("$binary" --version)" = "cargo-deny $version"
"$binary" --log-level warn --manifest-path ./Cargo.toml --all-features check
