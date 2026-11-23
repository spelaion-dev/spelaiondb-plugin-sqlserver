//! Babelfish for PostgreSQL through this plugin (SpelaionDB's SDB-313): SQL Server's protocol in
//! front of PostgreSQL. Against the image SpelaionDB builds in `testing/docker/babelfish`, with
//! `SPELAION_TEST_BABELFISH=127.0.0.1:11434`; without it the tests are skipped, which does not
//! count as tested. What it does not support, the server says: plans (`DBCC USEROPTIONS`) and
//! `WAITFOR` among them.

use std::time::Duration;

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, LogicalType, Secret,
    TlsMode, TransactionState, Value,
};
use spelaion_driver_sqlserver::SqlServerDriver;

fn params(database: &str) -> Option<ConnectParams> {
    let Ok(address) = std::env::var("SPELAION_TEST_BABELFISH") else {
        eprintln!("SPELAION_TEST_BABELFISH not set: Babelfish not tested");
        return None;
    };
    let (host, port) = address.rsplit_once(':').unwrap();
    Some(ConnectParams {
        host: host.into(),
        port: port.parse().unwrap(),
        user: "babelfish_user".into(),
        password: Some(Secret::new("Spelaion-test-1")),
        database: database.into(),
        tls: TlsMode::Require,
        ..Default::default()
    })
}

/// The first value of each row of every result of `sql`, as text, read by its column's type.
async fn values(conn: &dyn Connection, sql: &str) -> Result<Vec<String>, DriverError> {
    let events: Vec<ExecEvent> = conn
        .execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await?;
    let mut out = Vec::new();
    let mut logical = LogicalType::Text;
    for event in events {
        if let ExecEvent::Columns(schema) = &event {
            logical = schema.columns[0].logical;
        }
        if let ExecEvent::Batch(batch) = event {
            for row in 0..batch.num_rows() {
                out.push(
                    Value::from_array(batch.column(0), row, logical)
                        .map(|v| v.to_string())
                        .unwrap_or_default(),
                );
            }
        }
    }
    Ok(out)
}

async fn ok(conn: &dyn Connection, sql: &str) -> Vec<String> {
    values(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

#[tokio::test]
async fn babelfish_logs_in_runs_batches_and_answers_the_catalog() {
    let Some(params) = params("master") else {
        return;
    };
    // The login is encrypted (`require`): the image has a certificate of its own.
    let conn = SqlServerDriver
        .connect(&params)
        .await
        .unwrap_or_else(|e| panic!("login: {e}"));
    let version = ok(&*conn, "SELECT @@VERSION").await;
    assert!(
        version[0].starts_with("Babelfish for PostgreSQL"),
        "{version:?}"
    );
    ok(
        &*conn,
        "IF DB_ID('spelaion') IS NULL CREATE DATABASE spelaion",
    )
    .await;
    ok(&*conn, "USE spelaion").await;
    assert_eq!(
        conn.current_database().await.unwrap().as_deref(),
        Some("spelaion")
    );
    ok(
        &*conn,
        "IF OBJECT_ID('dbo.bf_t') IS NOT NULL DROP TABLE dbo.bf_t; \
         CREATE TABLE dbo.bf_t (id int PRIMARY KEY, name nvarchar(50) NULL, amount decimal(10, 2)); \
         INSERT INTO dbo.bf_t VALUES (1, N'ñandú', 1.50), (2, NULL, 2.00)",
    )
    .await;
    // A batch with two results, and a rolled-back transaction that leaves nothing.
    assert_eq!(ok(&*conn, "SELECT 1 AS a; SELECT 2 AS b").await, ["1", "2"]);
    assert_eq!(
        ok(
            &*conn,
            "BEGIN TRANSACTION; INSERT INTO dbo.bf_t VALUES (3, N'x', 0); ROLLBACK; \
             SELECT COUNT(*) FROM dbo.bf_t"
        )
        .await,
        ["2"]
    );
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Idle
    );
    // Babelfish sends no transaction descriptor: the state comes from the server.
    ok(&*conn, "BEGIN TRANSACTION").await;
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Open
    );
    assert_eq!(
        conn.keep_alive().await.unwrap(),
        spelaion_driver_api::KeepAlive::InTransaction
    );
    ok(&*conn, "ROLLBACK").await;
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Idle
    );

    // The catalog, as Babelfish emulates SQL Server's.
    assert!(conn.list_databases().await.unwrap().len() >= 4);
    assert!(
        conn.list_schemas()
            .await
            .unwrap()
            .iter()
            .any(|s| s.name == "dbo")
    );
    assert!(
        conn.list_objects("dbo")
            .await
            .unwrap()
            .iter()
            .any(|o| o.name == "bf_t")
    );
    let table = conn.describe_table("dbo", "bf_t").await.unwrap();
    let columns: Vec<String> = table
        .columns
        .iter()
        .map(|c| format!("{} {}", c.name, c.native_type))
        .collect();
    assert_eq!(
        columns,
        ["id int", "name nvarchar(50)", "amount decimal(10,2)"]
    );
    assert!(conn.list_server_objects().await.is_ok());

    // Bound values, as the data search sends them.
    let events: Vec<ExecEvent> = conn
        .execute_bound(
            "SELECT name FROM dbo.bf_t WHERE id = @P1",
            vec!["1".into()],
            &ExecOptions::default(),
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ExecEvent::Batch(b) if b.num_rows() == 1))
    );

    // An import's rows, through the TDS bulk load (a value with a tab, and a NULL).
    let target = spelaion_driver_api::InsertTarget {
        schema: "dbo".into(),
        table: "bf_t".into(),
        columns: vec!["id".into(), "name".into(), "amount".into()],
    };
    let mut rows = spelaion_driver_api::TextRows::builder(3);
    rows.push([Some("10"), Some("tab\there"), Some("3.25")].into_iter())
        .unwrap();
    rows.push([Some("11"), None, Some("0")].into_iter())
        .unwrap();
    let inserted = conn.insert_rows(&target, rows.finish()).await;
    eprintln!("Babelfish bulk load: {inserted:?}");
    assert_eq!(inserted.unwrap(), 2);
    assert_eq!(
        ok(&*conn, "SELECT name FROM dbo.bf_t WHERE id = 10").await,
        ["tab\there"]
    );

    // No plans: the plugin says so, and the server would refuse them anyway.
    assert!(!conn.capabilities().explain);
    let plan = match conn
        .execute_plan(
            "SELECT * FROM dbo.bf_t",
            spelaion_driver_api::PlanMode::Estimated,
        )
        .await
    {
        Ok(stream) => stream.try_collect::<Vec<_>>().await.map(|_| ()),
        Err(e) => Err(e),
    };
    assert!(
        plan.as_ref().is_err_and(|e| e
            .to_string()
            .contains("not currently supported in Babelfish")),
        "{plan:?}"
    );
    assert_eq!(ok(&*conn, "SELECT 42").await, ["42"]);
}

#[tokio::test]
async fn a_cancel_stops_a_babelfish_query_and_keeps_the_session() {
    let Some(params) = params("master") else {
        return;
    };
    let conn = SqlServerDriver.connect(&params).await.unwrap();
    // No `WAITFOR` here: a query that takes long enough instead.
    let running = conn
        .execute(
            "SELECT COUNT_BIG(*) FROM sys.all_objects a CROSS JOIN sys.all_objects b \
             CROSS JOIN sys.all_objects c",
            &ExecOptions::default(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let started = std::time::Instant::now();
    conn.cancel_handle().cancel().await.unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(15), running.try_collect::<Vec<_>>())
        .await
        .expect("the cancel stopped the statement");
    eprintln!("Babelfish cancel: {ended:?} in {:?}", started.elapsed());
    match ended {
        // The login may signal its own backends: the statement alone stopped.
        Err(DriverError::Cancelled) => assert_eq!(ok(&*conn, "SELECT 7").await, ["7"]),
        // It may not (the test login may not): its session was ended, saying so, and nothing
        // more runs on it; a new one works.
        Err(DriverError::Connection(message)) => {
            assert!(message.contains("session was ended"), "{message}");
            assert!(conn.is_closed());
            let again = SqlServerDriver.connect(&params).await.unwrap();
            assert_eq!(ok(&*again, "SELECT 7").await, ["7"]);
        }
        other => panic!("{other:?}"),
    }
    // Nothing of it keeps running on the server.
    let other = SqlServerDriver.connect(&params).await.unwrap();
    let running = ok(
        &*other,
        "SELECT CAST(COUNT(*) AS int) FROM pg_catalog.pg_stat_activity \
         WHERE state = 'active' AND query LIKE 'SELECT COUNT_BIG(*) FROM sys.all_objects%'",
    )
    .await;
    assert_eq!(running, ["0"]);
}
