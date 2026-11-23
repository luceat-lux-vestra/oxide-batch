#!/usr/bin/env bash
set -euo pipefail

test "$RUNNER_ARCH" = ARM64
test "$(uname -m)" = aarch64

repo_root="$(git rev-parse --show-toplevel)"
resolver="$repo_root/.github/scripts/validate_dependency_update_policy.py"
target="aarch64-linux"
version="$(python3 "$resolver" --root "$repo_root" --entry mold-arm64 --field version)"
url="$(python3 "$resolver" --root "$repo_root" --entry mold-arm64 --artifact "$target" --field url)"
sha256="$(python3 "$resolver" --root "$repo_root" --entry mold-arm64 --artifact "$target" --field sha256)"

archive="$RUNNER_TEMP/mold-${version}-${target}.tar.gz"
install_dir="$RUNNER_TEMP/mold-${version}"

curl --fail --location --retry 3 --retry-delay 2 "$url" --output "$archive"
echo "$sha256  $archive" | sha256sum --check --strict

mkdir -p "$install_dir"
tar -xzf "$archive" -C "$install_dir" --strip-components=1
test "$("$install_dir/bin/mold" --version | awk '{print $1, $2}')" = "mold $version"

wrapper="$RUNNER_TEMP/oxide-mold-linker"
cat > "$wrapper" <<SH
#!/usr/bin/env bash
exec /usr/bin/cc -B"$install_dir/bin/" -fuse-ld=mold "\$@"
SH
chmod +x "$wrapper"

printf 'int main(void) { return 0; }\n' > "$RUNNER_TEMP/mold-probe.c"
"$wrapper" "$RUNNER_TEMP/mold-probe.c" -Wl,-v -o "$RUNNER_TEMP/mold-probe" \
  2>&1 | tee "$RUNNER_TEMP/mold-probe.log"
test -x "$RUNNER_TEMP/mold-probe"
grep -Fq "mold $version" "$RUNNER_TEMP/mold-probe.log"

echo "CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=$wrapper" >> "$GITHUB_ENV"
