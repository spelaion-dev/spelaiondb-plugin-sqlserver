# SpelaionDB's SQL Server plugin — rules for agents

SQL Server for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb): the engine's driver,
and a small program that serves it to SpelaionDB over its plugin protocol. Apache-2.0.

**The rules of SpelaionDB's repository apply here** (its `AGENTS.md`: all the code is written
by an agent, the compiler, the tests and CI are the only reviewer; everything in English;
sign-off; one task, one short branch; commit subjects that read well in a changelog). This file
says only what is different in this repository.

## What this is
- `src/`: the driver. It implements `Driver` and `Connection` of `spelaion-driver-api`
  (SpelaionDB's ADR-005). `tiberius` over rustls, one actor task per connection (ADR-017
  there). A batch may return several result sets. `datetime2`, `datetimeoffset` and `time` are
  text with their native type, exactly as the server writes them (ADR-005 and ADR-006 there).
  The explorer's objects are in `objects.rs` and `introspect.rs`, `ddl.rs` writes definitions
  and `plan.rs` reads the plans.
- `src/bin/spelaiondb-plugin-sqlserver.rs`: the program. It hands the driver to
  `spelaion_plugin_sdk::run` and nothing else.
- `tests/sqlserver.rs`: the driver's tests against a real server. `tests/conformance.rs`:
  SpelaionDB's conformance set against this program, and `plugin.toml` checked against what the
  program says of itself.
- `vendor/tiberius`: tiberius patched so a cancel (a TDS Attention) leaves the session where it
  should. **Read its `PATCHED.md` before upgrading.** It is under `.github/CODEOWNERS`.
- `testing/compose.yaml` and `ci/services.sh`: the test server (SQL Server 2022, Developer
  edition), the same one SpelaionDB's repository uses.
- `plugin.toml`: the protocol version, the engines and the lowest SpelaionDB of this plugin.

## A plugin is the wire, not the rules
SpelaionDB's core splits, classifies and guards statements, keeps secrets, opens tunnels and
writes the audit (ADR-041 there). T-SQL batches (`GO`, `BEGIN … END`) are split there, in its
`spelaion-dialect`: this plugin receives each batch as one statement. It connects, runs what it
is sent, cancels, introspects and loads rows. So, and these are not negotiable
(`docs/plugin-protocol.md` in SpelaionDB's repository, "What a plugin must never do"):

- **Standard output is the protocol and nothing else.** Never `println!`. What must be said
  goes to standard error, a line at a time.
- **The password arrives in `connect` and is written nowhere**: not to a log, an error, a
  file or the environment.
- **It runs what it is sent.** It never decides whether a statement is a read, and never
  changes one.
- **Nothing is run again** after a connection is lost.
- **It ends when its standard input closes** (the SDK does this).
- It reads no configuration file, keychain or store of SpelaionDB's, and opens no tunnel: it
  connects to `connect_addr` when it is given one and verifies TLS against `host`.

## The contract comes from SpelaionDB, by tag
`spelaion-driver-api`, `spelaion-dialect`, `spelaion-plugin-sdk` and, for the tests,
`spelaion-plugin-host` are git dependencies on a tag of SpelaionDB's repository
(`plugin-sdk-vX.Y.Z`), all four on the same tag. They are not on crates.io.

- **A protocol change is made there first** (the new version, with its tests), then here: move
  the tag, adapt, release. SpelaionDB then moves its pin.
- Arrow must stay the version SpelaionDB uses: batches cross between the two unchanged.
- `plugin.toml`'s `protocol` is what the SDK of that tag speaks; `tests/conformance.rs` fails
  if they differ.

## Releases
Pushing a tag `vX.Y.Z` (the version in `Cargo.toml`) runs the release every plugin shares
(the template's repository, called by commit): the four platforms, each archive with its
SHA-256, and notes that carry the pin in the form SpelaionDB's `plugins.toml` wants. **Nothing
is signed and no secret lives here**: a release reaches users only when SpelaionDB's
repository pins its hashes in a pull request of its own, whose CI runs SpelaionDB's tests
through that exact build (ADR-042 there).

## Branches
`main` is protected: it changes only through pull requests whose CI is green, and cannot be
force-pushed or deleted. A pull request that touches a file in `.github/CODEOWNERS` waits for
an owner. The work is tracked in SpelaionDB's tracker (Plane, project `SDB`): write the item's
identifier in the pull request's description, never in a commit subject.

## Before calling something done
- `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`.
- The tests against a real server (without the variable they are skipped, and that does
  **not** count as tested): `ci/services.sh` and
  `SPELAION_TEST_MSSQL=127.0.0.1:11433 cargo test`.
- `cargo deny check` if dependencies change.
- A change to how values are written is checked against SpelaionDB too: build this program and
  run SpelaionDB's tests through it (`SPELAION_DEV_PLUGIN_SQLSERVER=<this program>` with a
  development build there).
- If something could not be tested, say so.
