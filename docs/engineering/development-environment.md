# Development Environment

**State:** Accepted

## Required tools

- Git and GitHub CLI;
- rustup with the repository toolchain from `rust-toolchain.toml`;
- a Docker-compatible container runtime for PostgreSQL integration tests;
- `jq` for release metadata validation;
- PostgreSQL client tools when diagnosing migrations manually.

Optional Cargo tools are pinned in CI or a future tool manifest; contributors
must not need globally mutable “latest” installations to run the basic build.

## Bootstrap contract

A fresh clone must be able to:

1. install the pinned Rust toolchain automatically through rustup;
2. build and test without access to production credentials;
3. start an isolated disposable PostgreSQL instance;
4. run migrations and integration tests with one documented command;
5. remove test resources without touching unrelated containers or volumes.

The repository uses a Rust `xtask` crate for coordinated commands. This keeps
local/CI behavior cross-platform and reviewed with the source. Shell scripts
remain thin adapters and must support the stated platform matrix.

## Planned commands

| Command | Purpose |
| --- | --- |
| `cargo xtask doctor` | Verify toolchain, container runtime, ports, and tools |
| `cargo xtask check` | Run the required fast local quality suite |
| `cargo xtask test integration` | Start isolated PostgreSQL and run contracts |
| `cargo xtask test conformance` | Run compatibility scenarios |
| `cargo xtask test crash` | Run subprocess failure-injection scenarios |
| `cargo xtask docs` | Build rustdoc and validate documentation links/examples |
| `cargo xtask package` | Verify publish content without publishing |

`doctor`, `check`, and `package` are available now. Integration, conformance,
crash, and documentation-specific commands are added with their corresponding
test infrastructure.

## Environment and secrets

- Local overrides belong in ignored `.env`-style files; a sanitized example
  documents names and safe defaults.
- Tests generate unique database/schema names and credentials.
- CI receives only job-scoped credentials with least privilege.
- Tests never connect to a database unless its name/host matches an explicit
  safe test configuration.
- Destructive test cleanup resolves and prints the exact test resource before
  removal.

## Reproducibility

- The workspace commits `Cargo.lock`.
- Rust is pinned; the MSRV is tested separately.
- Database images use an explicit major and immutable digest in CI once chosen.
- Locale, timezone, and test clock are controlled.
- Fixtures are synthetic, small, versioned, and license-safe.
- Failed integration tests retain sanitized logs and identifiers sufficient for
  reproduction.

## Editor support

`.editorconfig` and `rustfmt.toml` define repository formatting. `clippy.toml`
keeps lint assumptions aligned with the MSRV. Editor-specific files are optional
and may be committed only when they do not impose a vendor or overwrite personal
preferences. rust-analyzer must work from the workspace root.
