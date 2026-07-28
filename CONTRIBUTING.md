# Contributing to OxideBatch

Thank you for helping build OxideBatch.

## Before contributing

- Use GitHub Discussions for general questions and early ideas.
- Search existing issues before opening a new one.
- Report security vulnerabilities privately as described in `SECURITY.md`.
- Significant behavior, compatibility, persistence, or public API changes
  require an RFC or architecture decision record before implementation.
- Read the
  [development and decision process](docs/project/development-process.md) for
  definition-of-ready, definition-of-done, RFC, and review requirements.
- Follow the [coding conventions](docs/engineering/coding-conventions.md) and
  [Rust API design guidelines](docs/api/design-guidelines.md).

## Development workflow

1. Create or claim an issue.
2. Branch from `main` using a documented branch prefix.
3. Keep the change focused and add appropriate tests.
4. Open a pull request using the repository template.
5. Resolve review conversations and ensure every required check passes.

Recommended branch names:

```text
feat/123-short-description
fix/123-short-description
docs/123-short-description
refactor/123-short-description
test/123-short-description
chore/123-short-description
spike/123-short-description
```

## Quality checks

Run these commands before opening a pull request:

```shell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

Production code must not use `unwrap`, `expect`, or `panic!` without an
explicitly reviewed exception. Unsafe code is forbidden by default.

## Commit and pull request titles

Use Conventional Commit-style pull request titles:

```text
feat(runtime): add chunk restart checkpoint
fix(repository): reject concurrent job instance creation
docs(governance): clarify release ownership
```

Pull requests are squash-merged. The pull request title becomes the commit
subject on `main`.

## Contribution license

Unless explicitly stated otherwise, any contribution intentionally submitted
for inclusion in OxideBatch is provided under the Apache License, Version 2.0,
as described in Section 5 of that license.
