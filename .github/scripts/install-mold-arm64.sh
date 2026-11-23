#!/usr/bin/env bash
set -euo pipefail

test "$RUNNER_ARCH" = ARM64
test "$(uname -m)" = aarch64
: "${RUNNER_TEMP:?GitHub runner temporary directory is required}"
: "${GITHUB_ENV:?GitHub environment file is required}"
registry=".github/dependency-update-lifecycle.json"
test -f "$registry"

mapfile -t metadata < <(python3 - "$registry" <<'PY'
import json
import re
import sys

data = json.load(open(sys.argv[1], encoding="utf-8"))
entry = next((item for item in data["entries"] if item["id"] == "mold"), None)
if entry is None:
    raise SystemExit("missing mold lifecycle entry")
pin = entry["pin"]
artifact = pin["artifacts"]["aarch64-linux"]
digest = artifact["sha256"]
if not re.fullmatch(r"[0-9a-f]{64}", digest):
    raise SystemExit("invalid mold SHA-256")
print(pin["repository"])
print(pin["version"])
print(digest)
PY
)

test "${#metadata[@]}" -eq 3
repository="${metadata[0]}"
version="${metadata[1]}"
expected_sha256="${metadata[2]}"
archive_name="mold-${version}-aarch64-linux.tar.gz"
archive="$RUNNER_TEMP/$archive_name"
install_dir="$RUNNER_TEMP/mold-${version}"

curl --proto '=https' --tlsv1.2 --fail --location --retry 3 --retry-delay 2 \
  "https://github.com/${repository}/releases/download/v${version}/${archive_name}" \
  --output "$archive"
printf '%s  %s\n' "$expected_sha256" "$archive" | sha256sum --check --strict

rm -rf "$install_dir"
mkdir -p "$install_dir"
tar -xzf "$archive" -C "$install_dir" --strip-components=1
test "$("$install_dir/bin/mold" --version | awk '{print $1, $2}')" = "mold $version"

wrapper="$RUNNER_TEMP/oxide-mold-linker"
cat > "$wrapper" <<EOF
#!/usr/bin/env bash
exec /usr/bin/cc -B"$install_dir/bin/" -fuse-ld=mold "\$@"
EOF
chmod 0755 "$wrapper"

printf 'int main(void) { return 0; }\n' > "$RUNNER_TEMP/mold-probe.c"
"$wrapper" "$RUNNER_TEMP/mold-probe.c" -Wl,-v -o "$RUNNER_TEMP/mold-probe" \
  2>&1 | tee "$RUNNER_TEMP/mold-probe.log"
test -x "$RUNNER_TEMP/mold-probe"
grep -Fq "mold $version" "$RUNNER_TEMP/mold-probe.log"

printf 'CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=%s\n' "$wrapper" >> "$GITHUB_ENV"
