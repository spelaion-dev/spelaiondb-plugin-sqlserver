//! SQL Server Express LocalDB (SDB-312): the automatic instance `MSSQLLocalDB`, started by the
//! plugin and reached through its named pipe with Windows authentication. On Windows this runs
//! wherever `SqlLocalDB` is installed (GitHub's Windows runners have it); without it the test
//! says so and stops, which does not count as tested. Elsewhere it checks the refusal.

#[cfg(windows)]
use std::time::Duration;

#[cfg(windows)]
use futures_util::TryStreamExt;
use spelaion_driver_api::{ConnectParams, Driver};
#[cfg(windows)]
use spelaion_driver_api::{Connection, DriverError, ExecEvent, ExecOptions, TlsMode, Value};
use spelaion_driver_sqlserver::SqlServerDriver;

const INSTANCE: &str = "(localdb)\\MSSQLLocalDB";

fn localdb(host: &str) -> ConnectParams {
    ConnectParams {
        host: host.into(),
        // No user: the Windows account that runs the test.
        ..Default::default()
    }
}

#[cfg(not(windows))]
#[tokio::test]
async fn localdb_is_refused_where_it_cannot_run() {
    let error = SqlServerDriver
        .connect(&localdb(INSTANCE))
        .await
        .err()
        .expect("no LocalDB here")
        .to_string();
    assert!(error.contains("only on Windows"), "{error}");
}

/// The first value of each row of `sql`, as text.
#[cfg(windows)]
async fn values(conn: &dyn Connection, sql: &str) -> Vec<String> {
    let events: Vec<ExecEvent> = conn
        .execute(sql, &ExecOptions::default())
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .try_collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    // The first column's type, as the result's header says.
    let mut logical = spelaion_driver_api::LogicalType::Text;
    for event in events {
        if let ExecEvent::Columns(schema) = &event {
            logical = schema.columns[0].logical;
        }
        if let ExecEvent::Batch(batch) = event {
            for row in 0..batch.num_rows() {
                let value = Value::from_array(batch.column(0), row, logical)
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                out.push(value);
            }
        }
    }
    out
}

/// TEMPORARY DIAGNOSTIC (SDB-462): each encryption level through the pipe, with the pipe's
/// bytes traced. Always fails, so its output shows.
#[cfg(windows)]
#[tokio::test]
async fn diag_tls_through_the_pipe() {
    use std::process::Command;
    if Command::new("SqlLocalDB.exe")
        .arg("versions")
        .output()
        .is_err()
    {
        return;
    }
    let started = Command::new("SqlLocalDB.exe")
        .args(["start", "MSSQLLocalDB"])
        .output()
        .unwrap();
    eprintln!(
        "DIAG start: {}",
        String::from_utf8_lossy(&started.stdout).trim()
    );
    let versions = Command::new("SqlLocalDB.exe")
        .arg("versions")
        .output()
        .unwrap();
    eprintln!(
        "DIAG versions: {}",
        String::from_utf8_lossy(&versions.stdout).trim()
    );
    tokio::time::sleep(Duration::from_secs(10)).await;
    // Off, On, Required, Strict.
    for tls in [
        TlsMode::Disable,
        TlsMode::Prefer,
        TlsMode::Require,
        TlsMode::VerifyCa,
    ] {
        eprintln!("DIAG ===== {tls:?}");
        let outcome = SqlServerDriver
            .connect(&ConnectParams {
                tls,
                connect_timeout: Some(Duration::from_secs(1)),
                ..localdb(INSTANCE)
            })
            .await;
        match outcome {
            Ok(conn) => {
                let encrypt = values(
                    &*conn,
                    "SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID",
                )
                .await;
                eprintln!("DIAG {tls:?}: connected, encrypt_option {encrypt:?}");
                let transport = values(
                    &*conn,
                    "SELECT net_transport + N' / ' + protocol_type + N' / ' + auth_scheme FROM sys.dm_exec_connections WHERE session_id = @@SPID",
                )
                .await;
                eprintln!("DIAG {tls:?}: transport {transport:?}");
                let log = values(
                    &*conn,
                    "CREATE TABLE #l (d datetime, p nvarchar(100), t nvarchar(4000));
                     INSERT #l EXEC xp_readerrorlog 0, 1, N'ertificate';
                     INSERT #l EXEC xp_readerrorlog 0, 1, N'ncrypt';
                     INSERT #l EXEC xp_readerrorlog 0, 1, N'SSL';
                     INSERT #l EXEC xp_readerrorlog 0, 1, N'TLS';
                     INSERT #l EXEC xp_readerrorlog 0, 1, N'accept connection';
                     INSERT #l EXEC xp_readerrorlog 0, 1, N'Microsoft SQL Server';
                     CREATE TABLE #all (d datetime, p nvarchar(100), t nvarchar(4000));
                     INSERT #all EXEC xp_readerrorlog 0, 1;
                     INSERT #l SELECT NULL, NULL, CONCAT(N'lines in the error log: ', COUNT(*)) FROM #all;
                     SELECT t FROM #l ORDER BY d",
                )
                .await;
                for line in log {
                    eprintln!("DIAG errorlog: {line}");
                }
            }
            Err(e) => eprintln!("DIAG {tls:?}: {e}"),
        }
    }
    panic!("diagnostic: see the output above");
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "TEMPORARY DIAGNOSTIC (SDB-462): it stops the instance the diagnostic uses"]
async fn localdb_is_started_and_reached_through_its_pipe() {
    use std::process::Command;
    if Command::new("SqlLocalDB.exe")
        .arg("versions")
        .output()
        .is_err()
    {
        eprintln!("SqlLocalDB is not installed: LocalDB not tested");
        return;
    }
    // Stopped first, so the plugin is what starts it.
    let stopped = Command::new("SqlLocalDB.exe")
        .args(["stop", "MSSQLLocalDB"])
        .output()
        .unwrap();
    eprintln!(
        "SqlLocalDB stop: {}",
        String::from_utf8_lossy(&stopped.stdout).trim()
    );

    // Each TLS setting, as the user may leave it: `disable` sends nothing encrypted (no
    // password travels), the others encrypt the login.
    let mut opened = Vec::new();
    for tls in [TlsMode::Prefer, TlsMode::Disable, TlsMode::Require] {
        let outcome = SqlServerDriver
            .connect(&ConnectParams {
                tls,
                ..localdb(INSTANCE)
            })
            .await;
        eprintln!(
            "LocalDB with tls {tls:?}: {}",
            match &outcome {
                Ok(_) => "connected".to_string(),
                Err(e) => e.to_string(),
            }
        );
        if let Ok(conn) = outcome {
            opened.push(conn);
        }
    }
    let info = Command::new("SqlLocalDB.exe")
        .args(["info", "MSSQLLocalDB"])
        .output()
        .unwrap();
    eprintln!(
        "SqlLocalDB info: {}",
        String::from_utf8_lossy(&info.stdout).trim()
    );
    assert_eq!(opened.len(), 3, "every TLS setting connects");
    let conn = opened.remove(0);
    assert_eq!(
        values(&*conn, "SELECT CAST(SERVERPROPERTY('IsLocalDB') AS int)").await,
        ["1"]
    );
    let login = values(&*conn, "SELECT SUSER_SNAME()").await;
    assert!(
        login[0].contains('\\'),
        "a Windows account logs in: {login:?}"
    );
    eprintln!("LocalDB: logged in as {}", login[0]);

    // A batch, a rolled-back transaction and the session going on.
    let _ = values(
        &*conn,
        "CREATE TABLE #t (id int, name nvarchar(50)); INSERT INTO #t VALUES (1, N'ñandú'); SELECT 1",
    )
    .await;
    let _ = values(
        &*conn,
        "BEGIN TRANSACTION; INSERT INTO #t VALUES (2, N'x'); ROLLBACK; SELECT 1",
    )
    .await;
    assert_eq!(
        values(&*conn, "SELECT name FROM #t ORDER BY id").await,
        ["ñandú"]
    );

    // A cancel through the pipe stops the batch and keeps the session.
    let running = conn
        .execute("WAITFOR DELAY '00:00:30'", &ExecOptions::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    conn.cancel_handle().cancel().await.unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(10), running.try_collect::<Vec<_>>())
        .await
        .expect("the cancel reached the server");
    assert!(matches!(ended, Err(DriverError::Cancelled)), "{ended:?}");
    assert_eq!(values(&*conn, "SELECT 42").await, ["42"]);

    // A second connection at once, to the running instance.
    let other = SqlServerDriver.connect(&localdb(INSTANCE)).await.unwrap();
    assert_eq!(values(&*other, "SELECT 7").await, ["7"]);

    // What is refused, and how.
    let failure = |params: ConnectParams| async move {
        SqlServerDriver
            .connect(&params)
            .await
            .err()
            .expect("refused")
            .to_string()
    };
    let error = failure(localdb("(localdb)\\a & calc")).await;
    assert!(error.contains("not a LocalDB instance name"), "{error}");
    let error = failure(localdb("(localdb)\\SpelaionNoSuchInstance")).await;
    assert!(error.contains("SpelaionNoSuchInstance"), "{error}");
    let error = failure(ConnectParams {
        connect_addr: Some("127.0.0.1:1".parse().unwrap()),
        ..localdb(INSTANCE)
    })
    .await;
    assert!(error.contains("not through a tunnel"), "{error}");
    let error = failure(ConnectParams {
        user: "sa".into(),
        tls: TlsMode::VerifyFull,
        ..localdb(INSTANCE)
    })
    .await;
    assert!(error.contains("no certificate to verify"), "{error}");
}
