# tiberius 0.13.0, patched for SpelaionDB

The [tiberius](https://crates.io/crates/tiberius) crate (MIT or Apache-2.0, see the licence
files), copied from its crates.io release with one change, used through `[patch.crates-io]`
in the workspace `Cargo.toml`. Only `src/`, the manifest, the README and the licences are kept.

## The change: cancelling (`Client::cancel_query`) left the session one response behind

After a TDS Attention, SQL Server 2022 first ends the cancelled response (a DONE with the
error bit) and then acknowledges the Attention in a message of its own (a DONE with
`DONE_ATTN`). `Connection::cancel_request` read only the first message, failed with "Never
got a DONE token acknowledging the Attention signal" and left the acknowledgement on the
wire: the next query read it as its own (empty) response, and every response after that was
the previous query's.

`cancel_request` (in `src/client/connection.rs`, marked "SpelaionDB patch") now:

1. remembers whether the cancelled response had been read to its end;
2. if not, discards the partial packet and drains that message on packet boundaries (a
   stream dropped in the middle of a token cannot be parsed further), returning at once if
   its last token already is the acknowledgement;
3. reads the next message for the acknowledgement.

`crates/spelaion-driver-sqlserver/tests/sqlserver.rs` checks a cancel while waiting and while
rows stream, and that later queries get their own results.

### And the transaction descriptor after a cancel (SDB-458)

Draining on packet boundaries skips every token left in the cancelled response, and with them a
transaction ENVCHANGE: a `BEGIN TRAN`, `COMMIT` or `ROLLBACK` the server ran before the
Attention. The client then kept the old TDS transaction descriptor, and SQL Server refused every
later request with error 3989 ("New request is not allowed to start because it should come with
valid transaction descriptor"), the one that would have ended the transaction included.

- The token stream (`src/tds/stream/token.rs`, `try_unfold`) marks the connection while it is
  inside a token (`Connection::set_mid_token`): from its type byte until it is decoded.
- `cancel_request` reads a response dropped *between* tokens on as tokens
  (`TokenStream::drain_cancelled`), so its ENVCHANGE tokens move the descriptor; a cancel
  while the server is still working (a `WAITFOR`) is always this case.
- A response dropped *inside* a token (rows streaming) can only be drained, as before. The
  connection then says the descriptor may be stale (`Client::transaction_descriptor_unknown`,
  until the next request), and the plugin proves it with one query, closing the session if
  the server refuses it.

The plugin's `tests/sqlserver.rs` (`a_cancel_after_begin_or_commit_keeps_the_transaction_
descriptor`) cancels both kinds of batch against a real server. The unit tests at the end of
`token.rs`'s `tests` module (`a_cancel_…`) feed both cases canned bytes. They are not run by this
workspace (tiberius is not a member, and its dev-dependencies are many): run them from a copy of
this folder with `[workspace]` added to its manifest and `cargo test --lib --no-default-features
--features tds80,rustls,winauth` (the TLS certificate tests there fail without upstream's
`docker/certs`, which this copy does not keep).

To upgrade: take the new release, re-apply the change if upstream has not fixed it (or drop
this folder and the `[patch.crates-io]` entry if it has), and run those tests.

## Also: lints allowed (`src/lib.rs`)

A path dependency's warnings are not capped as those of crates.io dependencies are, and with
the features SpelaionDB leaves out (Windows authentication) some code is unused: CI's
`-D warnings` would fail on code that is not ours. `#![allow(warnings)]` keeps them quiet.

## Also: transaction state without a query

`Client::in_transaction` drains a previously dropped response and exposes the TDS transaction descriptor, so an idle keep-alive never enters an open transaction. Test it against a real SQL Server before upgrading.

## Also: the server's informational messages (ADR-035)

Upstream decodes INFO tokens (`PRINT`, `RAISERROR` with severity 10 or less, "Changed database
context…") and only logs them. `TokenStream::get_info` (`src/tds/stream/token.rs`) now also keeps
each one in the connection's `Context` (at most `MAX_INFO_MESSAGES`, counting the rest), and
`Client::take_info_messages` hands them over and empties the buffer. `InfoMessage` and
`MAX_INFO_MESSAGES` are exported from `src/lib.rs`. They are kept on any path, also when the batch
fails before its first result set. `crates/spelaion-core/tests/server_messages.rs` checks `PRINT`, a
low-severity `RAISERROR` and messages before an error against a real SQL Server.

## Also: a CLR type's name (SDB-213)

A column of a CLR user-defined type comes with its database, schema and type name
(`UdtInfo`), but `Column` kept only `ColumnType::Udt`, so every such column looked alike:
`geometry`, `geography` and `hierarchyid` included. SpelaionDB's map needs to tell the first
two apart (geography stores latitude first). `Column` (`src/row.rs`) now also keeps the type's
name, `Column::udt_type_name()` returns it, and the metadata decoder (`src/tds/codec/token/
token_col_metadata.rs`) fills it in from `TypeInfo::Udt`. Nothing else reads it.
