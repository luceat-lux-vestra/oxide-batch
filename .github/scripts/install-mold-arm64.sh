#!/usr/bin/env bash
set -euo pipefail

test "$RUNNER_ARCH" = ARM64
test "$(uname -m)" = aarch64

archive="$RUNNER_TEMP/mold-3.0.0-aarch64-linux.tar.gz"
install_dir="$RUNNER_TEMP/mold-3.0.0"

curl --fail --location --retry 3 --retry-delay 2   "https://github.com/rui314/mold/releases/download/v3.0.0/mold-3.0.0-aarch64-linux.tar.gz"   --output "$archive"

echo "52c759d3689babaea4c42af2f31062a74ab83e17ffb5a8090232534c67b70577  $archive"   | sha256sum --check --strict

mkdir -p "$install_dir"
tar -xzf "$archive" -C "$install_dir" --strip-components=1
test "$("$install_dir/bin/mold" --version | awk '{print $1, $2}')" = "mold 3.0.0"

wrapper="$RUNNER_TEMP/oxide-mold-linker"
cat > "$wrapper" <<SH
#!/usr/bin/env bash
exec /usr/bin/cc -B"$install_dir/bin/" -fuse-ld=mold "\$@"
SH
chmod +x "$wrapper"

printf 'int main(void) { return 0; }\n' > "$RUNNER_TEMP/mold-probe.c"
"$wrapper" "$RUNNER_TEMP/mold-probe.c" -Wl,-v -o "$RUNNER_TEMP/mold-probe"   2>&1 | tee "$RUNNER_TEMP/mold-probe.log"
test -x "$RUNNER_TEMP/mold-probe"
grep -Eiq 'mold[[:space:]]+3\.0\.0|mold.*3\.0\.0' "$RUNNER_TEMP/mold-probe.log"

echo "CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=$wrapper" >> "$GITHUB_ENV"
