#!/usr/bin/env bash
# Install the reviewed cargo-cyclonedx release binary from the lifecycle registry.
# The protected release-tag workflow runs on ubuntu-latest (Linux x86_64).
# Never execute a downloaded installer; verify the archive before extraction.
set -euo pipefail

target="x86_64-unknown-linux-gnu"
registry=".github/dependency-update-lifecycle.json"
test -f "$registry"

mapfile -t metadata < <(python3 - "$registry" "$target" <<'PY'
import json
import re
import sys

registry_path, target = sys.argv[1:]
data = json.load(open(registry_path, encoding="utf-8"))
entry = next((item for item in data["entries"] if item["id"] == "cargo-cyclonedx"), None)
if entry is None:
    raise SystemExit("missing cargo-cyclonedx lifecycle entry")
pin = entry["pin"]
artifact = pin["artifacts"].get(target)
if artifact is None:
    raise SystemExit(f"unsupported cargo-cyclonedx target: {target}")
digest = artifact["sha256"]
if not re.fullmatch(r"[0-9a-f]{64}", digest):
    raise SystemExit("invalid cargo-cyclonedx SHA-256")
print(pin["repository"])
print(pin["version"])
print(digest)
PY
)

test "${#metadata[@]}" -eq 3
repository="${metadata[0]}"
version="${metadata[1]}"
expected_sha256="${metadata[2]}"

if [ "$(uname -s)" != "Linux" ] || [ "$(uname -m)" != "x86_64" ]; then
  echo "::error::cargo-cyclonedx $version installer requires Linux x86_64" >&2
  exit 2
fi

: "${RUNNER_TEMP:?GitHub runner temporary directory is required}"
work_dir="$(mktemp -d "$RUNNER_TEMP/cargo-cyclonedx-install.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT
archive="$work_dir/cargo-cyclonedx-$target.tar.xz"
extract_root="$work_dir/extract"
install_root="$RUNNER_TEMP/cargo-cyclonedx-$version"
bin_dir="$install_root/bin"

curl --proto '=https' --tlsv1.2 --fail --location --retry 3 --retry-delay 2 \
  "https://github.com/${repository}/releases/download/cargo-cyclonedx-${version}/cargo-cyclonedx-${target}.tar.xz" \
  --output "$archive"
printf '%s  %s\n' "$expected_sha256" "$archive" | sha256sum --check --strict

mkdir -p "$extract_root"
tar --extract --xz --file "$archive" --directory "$extract_root"

mapfile -t binaries < <(find "$extract_root" -type f -name cargo-cyclonedx -print)
if [ "${#binaries[@]}" -ne 1 ]; then
  echo "::error::Expected exactly one cargo-cyclonedx binary; found ${#binaries[@]}" >&2
  exit 3
fi

rm -rf "$install_root"
mkdir -p "$bin_dir"
install -m 0755 "${binaries[0]}" "$bin_dir/cargo-cyclonedx"
version_output="$(PATH="$bin_dir:$PATH" cargo cyclonedx --version)"
case "$version_output" in
  *"$version"*) ;;
  *)
    echo "::error::Expected cargo-cyclonedx version $version, observed: $version_output" >&2
    exit 4
    ;;
esac

: "${GITHUB_PATH:?GitHub path file is required}"
printf '%s\n' "$bin_dir" >> "$GITHUB_PATH"
echo "Verified official cargo-cyclonedx $version ($target): $version_output"
