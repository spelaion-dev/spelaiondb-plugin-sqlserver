//! Babelfish for PostgreSQL (SpelaionDB's SDB-313): SQL Server's protocol in front of
//! PostgreSQL. The plugin speaks to it as to SQL Server, except where it differs, which it tells
//! from `@@VERSION` once the session is open:
//!
//! - It sends no transaction descriptor (the ENVCHANGE that opens a transaction), so whether a
//!   transaction is open is always asked to the server (`XACT_STATE()`).
//! - It answers a TDS Attention only once the running statement has ended on its own (a long
//!   query ran its whole three minutes). A cancel also stops the statement from a short
//!   connection of its own, before the Attention: with PostgreSQL's `pg_cancel_backend` where the
//!   login may signal its backends, which keeps the session as on SQL Server, otherwise with
//!   `KILL`, which ends it, saying so. `@@SPID` is the PostgreSQL backend's process id.
//! - It has no plans (`DBCC USEROPTIONS` and `SET SHOWPLAN_XML` are not supported).

use spelaion_driver_api::{ConnectParams, Result};

use super::{SqlClient, open, rows};

/// What a Babelfish session needs to be cancelled: how to open another session, and its own
/// backend's process id.
pub struct Babelfish {
    params: ConnectParams,
    spid: String,
}

/// What starts Babelfish's `@@VERSION`.
const VERSION_PREFIX: &str = "Babelfish for PostgreSQL";

impl Babelfish {
    /// The session's server, when it is Babelfish. Any failure to ask is taken for SQL Server,
    /// whose behaviour the plugin keeps.
    pub async fn recognise(client: &mut SqlClient, params: &ConnectParams) -> Option<Self> {
        let answer = rows(
            client,
            "SELECT @@VERSION, CAST(@@SPID AS nvarchar(20))",
            &[],
        )
        .await
        .ok()?;
        let row = answer.first()?;
        let version = row.first()?.as_deref()?;
        let spid = row.get(1)?.clone()?;
        (version.starts_with(VERSION_PREFIX) && spid.bytes().all(|b| b.is_ascii_digit())).then(
            || Babelfish {
                params: params.clone(),
                spid,
            },
        )
    }

    /// Stops this session's running statement, from a connection of its own: PostgreSQL's
    /// `pg_cancel_backend`, which keeps the session, where the login may signal its own backends
    /// (T-SQL runs as the database's user, which usually may not); otherwise `KILL`, which ends
    /// the session (allowed for one's own sessions).
    pub async fn stop_running(&self) -> Result<Stopped> {
        let mut other = open(&self.params).await?;
        let cancelled = rows(
            &mut other,
            "SELECT CAST(pg_cancel_backend(CAST(@P1 AS int)) AS int)",
            std::slice::from_ref(&self.spid),
        )
        .await;
        if let Ok(answer) = &cancelled
            && answer
                .first()
                .and_then(|row| row.first())
                .and_then(|v| v.as_deref())
                == Some("1")
        {
            return Ok(Stopped::Statement);
        }
        // Digits only (checked when it was read): nothing else is written into the statement.
        rows(&mut other, &format!("KILL {}", self.spid), &[]).await?;
        Ok(Stopped::Session)
    }
}

/// What [`Babelfish::stop_running`] had to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stopped {
    /// The statement alone: the session goes on.
    Statement,
    /// The whole session: it is gone, with any transaction it had open.
    Session,
}

/// What a cancel that ended the session says.
pub const SESSION_ENDED: &str = "Babelfish could not stop the running statement on its own, so \
    its session was ended (KILL), with any transaction it had open: a new session starts when \
    the connection is used again";
