# PostgreSQL M2 Design-Gate Fixture

This disposable fixture makes the M2 physical schema, roles, validated TLS,
schema-version rejection, and backup/restore rules executable before the
production adapter exists.

Run one supported major:

```console
./tests/fixtures/postgres/run-design-gate.sh 18
```

The CI matrix runs explicit `15`, `16`, `17`, and `18` image tags. The script
creates a private CA and a `localhost` server certificate, connects SQLx with
Rustls `verify-full`, applies the draft schema as the migrator role, executes
DML as the runtime role, proves runtime DDL is denied, checks read-only
operator access, rejects a simulated newer schema, and restores a `pg_dump`
backup into a clean database.

All passwords are fixed, disposable fixture values. The database binds only to
loopback and the container, volume, temporary certificates, and dump are
removed on exit. Never reuse these credentials or certificates.

`0001_draft_metadata.sql` is design evidence, not a released migration. Issue
#41 owns promotion into the adapter's immutable migration directory.
