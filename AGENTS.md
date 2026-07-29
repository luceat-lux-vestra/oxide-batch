# Agent Instructions

Before changing architecture, compatibility, roadmap, public APIs, repository
semantics, durable metadata, or distributed behavior, read in order:

1. `docs/README.md`;
2. `docs/project/post-m5-full-parity-strategy.md`;
3. `docs/product/vision-and-scope.md`;
4. `docs/roadmap.md`;
5. `docs/compatibility/spring-batch.md`;
6. `docs/compatibility/conformance-matrix.md`;
7. `docs/architecture/overview.md`;
8. the focused canonical document for the subsystem;
9. relevant accepted RFCs/ADRs and milestone gates.

Chat or session instructions never override accepted repository documents.
When authoritative documents conflict, stop dependent implementation and
produce an RFC/ADR or documentation correction. Never infer that a proposed
document is accepted.

Implementation pull requests must update affected feature-ledger rows and
evidence links. Record decisions, limits, migrations, and failure semantics in
their canonical repository documents so a later session can understand the
change without access to chat history.
