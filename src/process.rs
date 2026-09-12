use std::collections::VecDeque;
use std::ffi::OsString;
use std::process::Stdio;

use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

use crate::adapter::CommandSpec;

const SAFE_ENVIRONMENT: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TERM",
    "COLORTERM",
    "NO_COLOR",
    "TMPDIR",
    "TMP",
    "TEMP",
    "XDG_CONFIG_HOME",
    "XDG_CACHE_HOME",
    "XDG_DATA_HOME",
    "XDG_STATE_HOME",
    "CLAUDE_HOME",
    "CODEX_HOME",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
];

pub(crate) fn spawn(spec: &CommandSpec, cwd: &std::path::Path) -> std::io::Result<Child> {
    let mut command = Command::new(&spec.program);
    command.args(&spec.args).current_dir(cwd);
    if spec.clear_environment {
        let preserved = SAFE_ENVIRONMENT
            .iter()
            .filter_map(|name| std::env::var_os(name).map(|value| (OsString::from(name), value)));
        command.env_clear().envs(preserved);
    }
    command.envs(&spec.environment);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    command.spawn()
}

/// Kills the entire provider process group when a turn future is cancelled or
/// times out. Provider CLIs commonly launch shell and tool grandchildren, so
/// killing only the immediate child would leave work running in the sandbox.
pub(crate) struct ProcessTreeGuard {
    #[cfg(unix)]
    process_group: Option<i32>,
    #[cfg(windows)]
    process_id: Option<u32>,
}

impl ProcessTreeGuard {
    pub(crate) fn for_child(child: &Child) -> Self {
        Self {
            #[cfg(unix)]
            process_group: child.id().and_then(|id| i32::try_from(id).ok()),
            #[cfg(windows)]
            process_id: child.id(),
        }
    }

    pub(crate) fn disarm(&mut self) {
        #[cfg(unix)]
        {
            self.process_group = None;
        }
        #[cfg(windows)]
        {
            self.process_id = None;
        }
    }

    pub(crate) fn terminate(&mut self) {
        #[cfg(unix)]
        if let Some(process_group) = self.process_group.take() {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(-process_group),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
        #[cfg(windows)]
        if let Some(process_id) = self.process_id.take() {
            let _ = std::process::Command::new("taskkill")
                .args(["/PID", &process_id.to_string(), "/T", "/F"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

impl Drop for ProcessTreeGuard {
    fn drop(&mut self) {
        self.terminate();
    }
}

pub(crate) async fn bounded_stderr<R>(mut stderr: R, capacity: usize) -> std::io::Result<String>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut tail = VecDeque::with_capacity(capacity);
    let mut buffer = [0_u8; 4096];
    loop {
        let read = stderr.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        for byte in &buffer[..read] {
            if tail.len() == capacity {
                tail.pop_front();
            }
            tail.push_back(*byte);
        }
    }
    Ok(String::from_utf8_lossy(&tail.into_iter().collect::<Vec<_>>()).into_owned())
}
