# spelaiondb-plugin-sqlserver

SQL Server for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), the desktop database
client: the engine's driver, served to SpelaionDB as a plugin.

SpelaionDB's engines are programs it starts and talks to over their standard input and output
(its ADR-041). This is the one for SQL Server. It is built here, in its own repository, and
SpelaionDB pins by hash the exact release it was tested with (its ADR-042): you do not install
this by hand, it comes with SpelaionDB.

- **Protocol:** [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
  version 1.0. Experimental until SpelaionDB 1.0.
- **Engine:** `sqlserver`. TDS over rustls (`tiberius`, patched: `vendor/tiberius/PATCHED.md`).
- **License:** Apache-2.0.

## Working on it

```bash
ci/services.sh
```

starts the test server (SQL Server 2022 in Docker), and

```bash
SPELAION_TEST_MSSQL=127.0.0.1:11433 cargo test
```

runs the driver's tests and the protocol's conformance set against this plugin's own program.
Without the variable the tests that need the server are skipped, which does not count as tested.

To try a build with SpelaionDB, point a development build of it at the program
(`SPELAION_DEV_PLUGIN_SQLSERVER=/path/to/target/debug/spelaiondb-plugin-sqlserver`, in
SpelaionDB's repository; a release build ignores that variable).

`AGENTS.md` has the rules. Reports and questions go to
[SpelaionDB's issues](https://github.com/spelaion-dev/spelaiondb/issues).
