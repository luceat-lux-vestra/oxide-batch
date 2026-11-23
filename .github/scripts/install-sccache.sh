#!/usr/bin/env bash
set -euo pipefail

target="${1:?usage: install-sccache.sh <target-triple> [path|cargo-bin]}"
mode="${2:-path}"
registry=".github/dependency-update-lifecycle.json"
test -f "$registry"
: "${RUNNER_TEMP:?GitHub runner temporary directory is required}"

mapfile -t metadata < <(python3 - "$registry" "$target" <<'PY'
import json
import re
import sys

registry_path, target = sys.argv[1:]
data = json.load(open(registry_path, encoding="utf-8"))
entry = next((item for item in data["entries"] if item["id"] == "sccache"), None)
if entry is None:
    raise SystemExit("missing sccache lifecycle entry")
pin = entry["pin"]
version = pin["version"]
repository = pin["repository"]
artifact = pin["artifacts"].get(target)
if artifact is None:
    raise SystemExit(f"unsupported sccache target: {target}")
sha256 = artifact["sha256"]
if not re.fullmatch(r"[0-9a-f]{64}", sha256):
    raise SystemExit("invalid sccache SHA-256")
print(repository)
print(version)
print(sha256)
PY
)

test "${#metadata[@]}" -eq 3
repository="${metadata[0]}"
version="${metadata[1]}"
expected_sha256="${metadata[2]}"
archive_name="sccache-v${version}-${target}.tar.gz"
archive="$RUNNER_TEMP/$archive_name"
install_dir="$RUNNER_TEMP/sccache-v${version}"

curl --proto '=https' --tlsv1.2 --fail --location --retry 3 --retry-delay 2 \
  "https://github.com/${repository}/releases/download/v${version}/${archive_name}" \
  --output "$archive"
printf '%s  %s\n' "$expected_sha256" "$archive" | sha256sum --check --strict

rm -rf "$install_dir"
mkdir -p "$install_dir"
tar -xzf "$archive" -C "$install_dir" --strip-components=1
test -x "$install_dir/sccache"
test "$("$install_dir/sccache" --version)" = "sccache $version"

case "$mode" in
  path)
    : "${GITHUB_PATH:?GitHub path file is required}"
    printf '%s\n' "$install_dir" >> "$GITHUB_PATH"
    ;;
  cargo-bin)
    cargo_bin_dir="$(dirname "$(command -v cargo)")"
    test -d "$cargo_bin_dir"
    install -m 0755 "$install_dir/sccache" "$cargo_bin_dir/sccache"
    ;;
  *)
    echo "unsupported install mode: $mode" >&2
    exit 2
    ;;
esac

echo "Verified sccache $version ($target)"
