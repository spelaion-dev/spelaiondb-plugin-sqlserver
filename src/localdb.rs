use spelaion_driver_api::{ConnectParams, DriverError, Result, TlsMode};
use tiberius::Config;

use super::Transport;

/// The SQL Server-specific connection option defined in SpelaionDB's ADR-066.
pub(crate) const INSTANCE: &str = "localdb_instance";

fn check_target(params: &ConnectParams, instance: &str) -> Result<()> {
    if instance.chars().count() > 128
        || !instance.starts_with(|ch: char| ch.is_alphanumeric() || ch == '_')
        || instance.trim_end() != instance
        || !instance
            .chars()
            .all(|ch| ch.is_alphanumeric() || matches!(ch, '_' | '-' | ' '))
    {
        return Err(DriverError::Connection(
            "invalid LocalDB instance name".into(),
        ));
    }
    if !params.host.is_empty()
        || params.port != 0
        || !params.user.is_empty()
        || params.password.is_some()
        || params.connect_addr.is_some()
        || params.remote.is_some()
        || params.tls != TlsMode::default()
        || params.ca_certificate.is_some()
        || params.client_certificate.is_some()
        || params.client_key.is_some()
    {
        return Err(DriverError::Connection(
            "LocalDB uses the current Windows user and cannot have a network address, SQL login, or TLS settings".into(),
        ));
    }
    Ok(())
}

pub(crate) async fn open(
    params: &ConnectParams,
    instance: &str,
) -> Result<(Config, Box<dyn Transport>)> {
    check_target(params, instance)?;
    #[cfg(not(windows))]
    return Err(DriverError::Connection(
        "SQL Server LocalDB is available only on Windows".into(),
    ));

    #[cfg(windows)]
    {
        use tiberius::{AuthMethod, EncryptionLevel};
        let stream = windows::connect(instance).await?;
        let mut config = super::common_config(params);
        config.host("localhost");
        config.authentication(AuthMethod::Integrated);
        // The OS binds this pipe to the local instance. No SQL password is transmitted.
        config.encryption(EncryptionLevel::Off);
        config.trust_cert();
        Ok((config, Box::new(stream)))
    }
}

/// The utility labels may be localized. Accept only its canonical local pipe value.
#[cfg(any(windows, test))]
fn instance_pipe(output: &str) -> Option<&str> {
    const PREFIX: &str = r"\\.\pipe\LOCALDB#";
    const SUFFIX: &str = r"\tsql\query";
    let mut pipes = output.split_whitespace().filter_map(|word| {
        let pipe = word.strip_prefix("np:")?;
        let id = pipe.strip_prefix(PREFIX)?.strip_suffix(SUFFIX)?;
        (!id.is_empty() && id.len() <= 64 && id.bytes().all(|c| c.is_ascii_hexdigit()))
            .then_some(pipe)
    });
    let pipe = pipes.next()?;
    pipes.next().is_none().then_some(pipe)
}

#[cfg(windows)]
mod windows {
    use std::{env, fs, io, path::PathBuf, process::Stdio, time::Duration};

    use spelaion_driver_api::{DriverError, Result};
    use tokio::{
        net::windows::named_pipe::{ClientOptions, NamedPipeClient},
        process::Command,
        time::sleep,
    };

    /// The utility shipped by LocalDB, in an installed SQL Server version's tools folder.
    /// Search neither the working directory nor the executable search path.
    fn installed_tool() -> Option<PathBuf> {
        ["ProgramW6432", "ProgramFiles", "ProgramFiles(x86)"]
            .into_iter()
            .filter_map(env::var_os)
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .flat_map(|root| {
                fs::read_dir(root.join("Microsoft SQL Server"))
                    .into_iter()
                    .flatten()
            })
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let version = entry.file_name().to_str()?.parse::<u32>().ok()?;
                let executable = entry.path().join(r"Tools\Binn\SqlLocalDB.exe");
                executable.is_file().then_some((version, executable))
            })
            .max_by_key(|(version, _)| *version)
            .map(|(_, executable)| executable)
    }

    async fn command(tool: &std::path::Path, action: &str, instance: &str) -> Result<String> {
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        let output = Command::new(tool)
            .args([action, instance])
            .stdin(Stdio::null())
            .creation_flags(CREATE_NO_WINDOW)
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|e| DriverError::Connection(format!("could not run SqlLocalDB: {e}")))?;
        if !output.status.success() {
            return Err(DriverError::Connection(format!(
                "SqlLocalDB {action} failed for {instance}; check that the instance exists for this Windows user"
            )));
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    pub(super) async fn connect(instance: &str) -> Result<NamedPipeClient> {
        let tool = installed_tool().ok_or_else(|| {
            DriverError::Connection(
                "SqlLocalDB.exe is missing; install SQL Server Express LocalDB".into(),
            )
        })?;
        command(&tool, "start", instance).await?;
        let info = command(&tool, "info", instance).await?;
        let pipe = super::instance_pipe(&info).ok_or_else(|| {
            DriverError::Connection("SqlLocalDB did not report a single local instance pipe".into())
        })?;
        loop {
            // Keep Tokio's default SECURITY_IDENTIFICATION impersonation level.
            match ClientOptions::new().open(pipe) {
                Ok(stream) => return Ok(stream),
                // A running instance may be accepting another connection on this pipe.
                Err(e) if e.raw_os_error() == Some(231) => sleep(Duration::from_millis(30)).await,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Err(DriverError::Connection(
                        "the LocalDB instance pipe was not found".into(),
                    ));
                }
                Err(e) => {
                    return Err(DriverError::Connection(format!(
                        "could not open the LocalDB instance pipe: {e}"
                    )));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_one_canonical_localdb_pipe_is_accepted() {
        let pipe = r"\\.\pipe\LOCALDB#F365A78E\tsql\query";
        assert_eq!(
            instance_pipe(&format!("Nombre de canalización: np:{pipe}\r\n")),
            Some(pipe)
        );
        assert_eq!(instance_pipe(&format!("np:{pipe}\r\nnp:{pipe}")), None);
        for value in [
            r"np:\\server\pipe\LOCALDB#F365A78E\tsql\query",
            r"np:\\.\pipe\openssh-ssh-agent",
            r"np:\\.\pipe\LOCALDB#F365A78E\tsql\query\extra",
            r"np:\\.\pipe\LOCALDB#..\tsql\query",
        ] {
            assert_eq!(instance_pipe(value), None);
        }
    }

    #[test]
    fn instance_names_cannot_supply_arguments_or_paths() {
        let params = ConnectParams::default();
        for valid in ["MSSQLLocalDB", "Development DB", "Développement_1"] {
            assert!(check_target(&params, valid).is_ok(), "{valid}");
        }
        for invalid in [
            "",
            "-s",
            "/?",
            " name",
            "name ",
            "two\nlines",
            "other\\name",
            "\"name\"",
        ] {
            assert!(check_target(&params, invalid).is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn an_instance_has_no_network_or_sql_authentication() {
        for params in [
            ConnectParams {
                host: "localhost".into(),
                ..Default::default()
            },
            ConnectParams {
                connect_addr: Some("127.0.0.1:1433".parse().unwrap()),
                ..Default::default()
            },
            ConnectParams {
                user: "sa".into(),
                ..Default::default()
            },
            ConnectParams {
                tls: TlsMode::Require,
                ..Default::default()
            },
        ] {
            assert!(check_target(&params, "MSSQLLocalDB").is_err());
        }
    }
}
