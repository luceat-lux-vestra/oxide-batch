# M5 PostgreSQL Security Campaign Fixture

This directory holds the M5 security campaign's denominator, its
least-privilege policy, and the script that builds the environment the campaign
needs.

Run one supported major:

```console
./tests/fixtures/security/provision.sh 18
```

The script generates a private certificate authority and a `localhost`
certificate, starts a `PostgreSQL` container with TLS configured against them,
starts a second container with TLS switched off, generates a second authority
that signs nothing, and then runs `cargo xtask security` against all of it.

If an environment already supplies those things, the runner can be used
directly:

```console
cargo xtask security
```

## What each file is

| File | What it holds |
| --- | --- |
| `campaign-scope.json` | The campaign's denominator: the reports, the fixtures they need, the TLS refusals, the privilege classes, and the surfaces the sweep must cover. |
| `roles.sql` | The five privilege classes, the cluster-level privileges they are denied, and the withdrawal of everything `PostgreSQL` grants to `PUBLIC` by default. |
| `grants.sql` | What each class may reach in the current metadata schema. |
| `provision.sh` | The environment the campaign cannot derive from a connection string. |

The policy is committed SQL rather than statements assembled by a test, so what
each class may do is reviewable on its own, and a change to it shows up as a
change to the policy rather than as a change to a test.

## Why the fixture is more than a database

Three of the campaign's claims cannot be checked against an ordinary server.

A certificate refusal needs an authority that really signed nothing the server
presents, so `provision.sh` generates a second one. A host-name refusal needs a
name that reaches the same server and is not in its certificate, so the
certificate carries `DNS:localhost` and nothing else and the campaign connects
to `127.0.0.1` for that attempt. And the claim that the supported mode does not
fall back to an unencrypted session needs a reachable server that offers no TLS
at all, which is the second container: without it the campaign would only show
that a bad certificate is refused, which a client that quietly continued
unencrypted whenever TLS was unavailable would also show.

## Credentials

Everything here is disposable. The certificates live for a day, both servers
bind to loopback, the containers and volume are removed on exit, and the
least-privilege classes are created without a password — the campaign sets one
generated for the run. Nothing in this directory is a credential, and the
retained evidence records no credential, connection string, certificate, or
canary.

## What this fixture is not

It does not replace `tests/fixtures/postgres/run-design-gate.sh`. That fixture
is M2 design evidence on its own CI axis over `PostgreSQL` 15 through 18: it
predates the adapter, exercises a draft schema through `psql`, and separates
four roles rather than the five the M5 preview does. This campaign runs the
shipped configuration path against the schema it was current for and is not the same evidence under
a new name.
