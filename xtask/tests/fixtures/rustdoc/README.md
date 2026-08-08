# Rustdoc fixtures

Real rustdoc output, committed so the surface inspection is tested against the
markup rustdoc actually emits rather than against markup this repository
invented. Synthetic pages prove the scanner's rules; these prove the rules
match reality.

`source.rs.txt` is the crate the pages were rendered from. Each item places a
foreign type in one position a signature can disclose one through: a public
field, an argument, a return, an associated-type bound, and a bound on a
method's type parameter. Its crate documentation also links to `docs.rs` in
prose, which the scan must not report.

Regenerate with the pinned toolchain when rustdoc's markup changes:

```shell
mkdir -p /tmp/surface-fixture/src
cp xtask/tests/fixtures/rustdoc/source.rs.txt /tmp/surface-fixture/src/lib.rs
cat > /tmp/surface-fixture/Cargo.toml <<'TOML'
[package]
name = "surface-fixture"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
serde_json = "1.0.151"
TOML
(cd /tmp/surface-fixture && RUSTUP_TOOLCHAIN=1.97.1 cargo doc --no-deps)
cp /tmp/surface-fixture/target/doc/surface_fixture/struct.Positions.html \
   /tmp/surface-fixture/target/doc/surface_fixture/trait.Bounded.html \
   xtask/tests/fixtures/rustdoc/
```

A regenerated fixture is reviewed like any other evidence: if the scan's result
changes with it, the change is in what rustdoc renders, and the scanner has to
account for it.
