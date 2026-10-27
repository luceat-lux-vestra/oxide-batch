# M7 operator fork audit evidence — #615

**State:** Accepted Gate-E implementation evidence for #615. #615 is complete
and this record is consumed by the completed #199 delivery; #200 remains the
M7-wide conformance, documentation, and exit-evidence owner.

## Boundary

This record covers only the Gate-E service integration of the already accepted
Gate-D fork lineage model. It does not redesign `ForkRequest`,
`ForkLineage`, definition registration, compatible upgrade, or explorer
queries.

The operator action is `FORK`, never `RESTART`.

## Request identity and redaction

`OperatorRequest::fork` targets the exact source execution/version carried by
one `ForkRequest`. Its canonical request digest binds:

- source execution identifier and optimistic version;
- target job name, definition revision/manifest identity and artifact identity;
- the value-free complete target-parameter digest;
- when present, the exact context-transform source step/version and durable
  transform descriptor, including source/target nodes, state family/schema
  versions, and transformer revision.

Raw parameter values, context payloads, executable transformer code, and
credentials are not copied into `ob_operator_request`.

## Transaction and replay model

The Gate-D fork implementation now exposes its mutation body only as a
crate-internal transaction-scoped staging primitive. Both callers reuse it:

- `JobFork` retains the existing direct fork API, collision reconciliation,
  unknown-commit readback, and immutable lineage semantics;
- `JobOperator` stages fork target creation/lineage and appends the ordinary
  M4 operator audit row in the same `RepositoryUnitOfWork`, then commits once.

Operator replay probes `(FORK, OperationId)` before any mutation. A concurrent
target-key collision rolls the losing unit back and re-reads the operator audit
rather than creating or adopting a second lineage. An existing Gate-D lineage
without an operator audit is not silently adopted as an atomic operator
operation.

A transformed fork follows the same live-code separation as compatible
restart: first application requires the exact process-local transformer
revision, while an already recorded operator outcome replays before live code
is inspected.

## PostgreSQL schema disposition

Schema 3 intentionally froze the then-known operator action vocabulary in the
`ob_operator_request.action` CHECK. A first-class `FORK` record therefore
cannot be correct without changing that closed set.

`0013_operator_fork_action.sql` advances the repository schema from 11 to 12
and changes only that action CHECK, adding `FORK`. It adds no table, column,
fork state family, or independent commit boundary. The populated schema-11
upgrade test proves the current migrator accepts a durable `FORK` audit row
after the migration.

Gate-D schema-11 fork lineage and PostgreSQL 15/18 process-kill evidence remain
authoritative for fork target/lineage durability. #615 adds only the operator
audit composition and schema-12 action-vocabulary migration; the normal PR
PostgreSQL design/conformance gates remain required before acceptance.

## Accepted tests

- `operator_fork_applies_once_and_replays_as_fork`
  - creates one fresh target lineage/instance/execution;
  - records `OperatorAction::Fork`;
  - replays without creating another execution.
- `operator_fork_digest_binds_source_target_and_parameter_identity`
  - proves source version, target artifact, and parameter identity affect the
    canonical operator digest.
- `schema11_operator_action_migration_accepts_explicit_fork`
  - upgrades a real schema-11 database to schema 12 and inserts/reads `FORK`
    through the constrained audit table.
- Existing Gate-D `fork_lineage`, `fork_transform`,
  `postgres_fork`, and `postgres_fork_process_kill` tests remain the
  underlying fork contract.

## Non-goals

- bounded explorer projection/query expansion;
- registry or compatibility-edge redesign;
- new fork lineage/state tables;
- compatibility inference or `Force`;
- CLI/API-server/hosted control-plane work;
- M8-M14 behavior.

At #615 acceptance, #199 still retained the bounded explorer slice. That
follow-on Gate-E slice is now complete; #200 owns final M7-wide conformance,
documentation reconciliation, and exit evidence.
