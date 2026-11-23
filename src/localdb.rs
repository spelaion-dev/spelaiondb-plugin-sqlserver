//! SQL Server Express LocalDB (SpelaionDB's SDB-312 and ADR-067): an instance of SQL Server on
//! Windows that listens only on a named pipe of its own, started on demand.
//!
//! A connection names it as SQL Server's own tools do, `(localdb)\MSSQLLocalDB`, in place of a
//! host. The plugin starts the instance with Microsoft's `SqlLocalDB` program (`start`, which
//! does nothing when it already runs), reads the instance's pipe from `SqlLocalDB info`, and
//! speaks TDS through that pipe. The program is the one on the PATH, run without a shell, and
//! its only arguments are the subcommand and the instance's name, checked first.

use spelaion_driver_api::{DriverError, Result};

/// What starts a LocalDB instance's name in place of a host.
const PREFIX: &str = "(localdb)\\";

/// The longest instance name LocalDB accepts.
const MAX_NAME: usize = 128;

/// The instance's name when `host` names a LocalDB instance (`(localdb)\MSSQLLocalDB`, the
/// prefix in any case), `None` for an ordinary host.
pub fn instance(host: &str) -> Option<&str> {
    let host = host.trim();
    let prefix = host.get(..PREFIX.len())?;
    prefix
        .eq_ignore_ascii_case(PREFIX)
        .then(|| &host[PREFIX.len()..])
}

/// An instance name as LocalDB takes it: letters, digits, spaces, `_`, `-` and `.`, up to 128
/// characters, starting with a letter, a digit or `_` (never an option's `-`). Nothing else
/// reaches `SqlLocalDB`'s command line.
pub fn check_name(name: &str) -> Result<()> {
    let ok = name
        .chars()
        .next()
        .is_some_and(|c| c.is_alphanumeric() || c == '_')
        && name.chars().count() <= MAX_NAME
        && name
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, ' ' | '_' | '-' | '.'));
    if ok {
        Ok(())
    } else {
        Err(DriverError::Connection(format!(
            "{name:?} is not a LocalDB instance name: use letters, digits, spaces, _, - and ., up to {MAX_NAME} characters"
        )))
    }
}

/// The instance's pipe (`\\.\pipe\LOCALDB#…\tsql\query`) in `SqlLocalDB info`'s output, which
/// Windows writes in the system's language: the one value that starts with `np:\\.\pipe\`.
#[cfg(any(windows, test))]
pub fn pipe_in(info: &str) -> Option<String> {
    let start = info.find("np:\\\\.\\pipe\\")?;
    let pipe: String = info[start + 3..]
        .chars()
        .take_while(|c| !c.is_whitespace())
        .collect();
    (pipe.len() > "\\\\.\\pipe\\".len()).then_some(pipe)
}

/// How long starting an instance may take. A cold start (the first one after the machine
/// starts, or after LocalDB was installed) takes tens of seconds, longer than a login: it has
/// this allowance of its own, apart from the connection's `connect_timeout`, so `SqlLocalDB`
/// is not stopped half way, which leaves the instance stopped.
#[cfg(windows)]
const START_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// How long the pipe may take to show in `SqlLocalDB info` once `start` has answered.
#[cfg(windows)]
const PIPE_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Starts the instance `name` if it is not running, and answers its pipe once it has one.
#[cfg(windows)]
pub async fn start(name: &str) -> Result<String> {
    check_name(name)?;
    let run = async {
        let started = sqllocaldb(&["start", name]).await?;
        if !started.status.success() {
            return Err(DriverError::Connection(format!(
                "could not start the LocalDB instance {name}: {}",
                said(&started)
            )));
        }
        // The pipe is in `info` once the instance runs, which may be a moment after `start`.
        let waiting = tokio::time::Instant::now();
        loop {
            let info = sqllocaldb(&["info", name]).await?;
            if let Some(pipe) = pipe_in(&String::from_utf8_lossy(&info.stdout)) {
                return Ok(pipe);
            }
            if !info.status.success() || waiting.elapsed() > PIPE_WAIT {
                return Err(DriverError::Connection(format!(
                    "the LocalDB instance {name} gave no pipe to connect to: {}",
                    said(&info)
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    };
    tokio::time::timeout(START_TIMEOUT, run)
        .await
        .map_err(|_| {
            DriverError::Connection(format!(
                "the LocalDB instance {name} did not start in {} s",
                START_TIMEOUT.as_secs()
            ))
        })?
}

/// LocalDB exists only on Windows.
#[cfg(not(windows))]
pub async fn start(name: &str) -> Result<String> {
    check_name(name)?;
    Err(DriverError::Connection(format!(
        "SQL Server LocalDB runs only on Windows: the instance {name} cannot be reached from this system"
    )))
}

/// Runs `SqlLocalDB` with `args`: no shell, no window, killed if the connection gives up.
#[cfg(windows)]
async fn sqllocaldb(args: &[&str]) -> Result<std::process::Output> {
    // No console window flashes up for it.
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    tokio::process::Command::new("SqlLocalDB.exe")
        .args(args)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .creation_flags(CREATE_NO_WINDOW)
        .output()
        .await
        .map_err(|e| {
            DriverError::Connection(format!(
                "could not run SqlLocalDB ({e}): install SQL Server Express LocalDB, which puts it on the PATH"
            ))
        })
}

/// What `SqlLocalDB` said: its error output, else its output, on one line.
#[cfg(windows)]
fn said(output: &std::process::Output) -> String {
    let err = String::from_utf8_lossy(&output.stderr);
    let out = String::from_utf8_lossy(&output.stdout);
    let text = if err.trim().is_empty() { out } else { err };
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_localdb_host_names_its_instance() {
        assert_eq!(instance("(localdb)\\MSSQLLocalDB"), Some("MSSQLLocalDB"));
        assert_eq!(instance("(LocalDB)\\v11.0"), Some("v11.0"));
        assert_eq!(instance(" (localdb)\\My App "), Some("My App"));
        assert_eq!(instance("db.internal"), None);
        assert_eq!(instance("(localdb)"), None);
        assert_eq!(instance("localdb\\x"), None);
        assert_eq!(instance(""), None);
        // A multibyte host shorter than the prefix is not cut in a character.
        assert_eq!(instance("ñ"), None);
    }

    #[test]
    fn only_plain_names_reach_the_command_line() {
        for ok in ["MSSQLLocalDB", "v11.0", "My App", "app_1-test", "Ñandú"] {
            assert!(check_name(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            " ",
            "a\"b",
            "a & calc",
            "a | b",
            "x\ny",
            "..\\x",
            "a/b",
            "-c",
            "$(x)",
            "a;b",
            " MSSQLLocalDB",
            ".x",
            &"x".repeat(129),
        ] {
            assert!(check_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_pipe_is_found_whatever_the_language() {
        let english = "Name:               MSSQLLocalDB\r\nVersion:            15.0.4153.1\r\n\
            State:              Running\r\nInstance pipe name: np:\\\\.\\pipe\\LOCALDB#3B4F2A1C\\tsql\\query\r\n";
        assert_eq!(
            pipe_in(english).as_deref(),
            Some("\\\\.\\pipe\\LOCALDB#3B4F2A1C\\tsql\\query")
        );
        let spanish = "Nombre:             MSSQLLocalDB\r\nEstado:             En ejecución\r\n\
            Nombre de canalización de instancia: np:\\\\.\\pipe\\LOCALDB#AB12\\tsql\\query\r\n";
        assert_eq!(
            pipe_in(spanish).as_deref(),
            Some("\\\\.\\pipe\\LOCALDB#AB12\\tsql\\query")
        );
        // Stopped: no pipe.
        assert_eq!(pipe_in("State: Stopped\r\nInstance pipe name: \r\n"), None);
        assert_eq!(pipe_in("np:\\\\.\\pipe\\"), None);
    }
}
