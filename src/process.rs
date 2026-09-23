use std::collections::VecDeque;
use std::ffi::OsStr;
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

// Windows tools locate their configuration and system executables through these
// variables. Keep this allowlist explicit: inheriting the entire environment
// would also expose unrelated API keys to providers and their tools.
const WINDOWS_SAFE_ENVIRONMENT: &[&str] = &[
    "SYSTEMROOT",
    "SYSTEMDRIVE",
    "WINDIR",
    "COMSPEC",
    "PATHEXT",
    "USERPROFILE",
    "HOMEDRIVE",
    "HOMEPATH",
    "APPDATA",
    "LOCALAPPDATA",
    "PROGRAMDATA",
    "PROGRAMFILES",
    "PROGRAMFILES(X86)",
];

fn allowed_environment_key(name: &OsStr, windows: bool) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    if windows {
        SAFE_ENVIRONMENT
            .iter()
            .chain(WINDOWS_SAFE_ENVIRONMENT)
            .any(|allowed| name.eq_ignore_ascii_case(allowed))
    } else {
        SAFE_ENVIRONMENT.contains(&name)
    }
}

#[cfg(windows)]
fn windows_program(spec: &CommandSpec) -> std::path::PathBuf {
    // CreateProcess only searches .exe for a bare name. npm installs .cmd
    // launchers; resolve those against the effective child PATH before letting
    // Rust handle batch-file argument escaping.
    if spec.program.components().count() != 1 || spec.program.extension().is_some() {
        return spec.program.clone();
    }
    let path = spec
        .environment
        .iter()
        .find(|(key, _)| key.to_string_lossy().eq_ignore_ascii_case("PATH"))
        .map(|(_, value)| value.clone())
        .or_else(|| std::env::var_os("PATH"));
    if let Some(path) = path {
        for directory in std::env::split_paths(&path) {
            for extension in ["exe", "cmd", "bat", "com"] {
                let candidate = directory.join(&spec.program).with_extension(extension);
                if candidate.is_file() {
                    return candidate;
                }
            }
        }
    }
    spec.program.clone()
}

pub(crate) fn spawn(spec: &CommandSpec, cwd: &std::path::Path) -> std::io::Result<Child> {
    #[cfg(windows)]
    let program = windows_program(spec);
    #[cfg(not(windows))]
    let program = spec.program.clone();
    let mut command = Command::new(program);
    command.args(&spec.args).current_dir(cwd);
    if spec.clear_environment {
        let preserved =
            std::env::vars_os().filter(|(name, _)| allowed_environment_key(name, cfg!(windows)));
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
    #[cfg(windows)]
    command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW for desktop hosts.
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
            use std::os::windows::process::CommandExt;
            let _ = std::process::Command::new("taskkill")
                .creation_flags(0x0800_0000)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn windows_environment_preserves_system_and_profile_but_not_credentials() {
        for name in [
            "Path",
            "SystemRoot",
            "ComSpec",
            "PATHEXT",
            "USERPROFILE",
            "AppData",
            "LOCALAPPDATA",
        ] {
            assert!(
                allowed_environment_key(OsStr::new(name), true),
                "missing {name}"
            );
        }
        for name in [
            "OPENAI_API_KEY",
            "ANTHROPIC_API_KEY",
            "AWS_SECRET_ACCESS_KEY",
            "FLEET_TOKEN",
            "UNRELATED_VARIABLE",
        ] {
            assert!(
                !allowed_environment_key(OsStr::new(name), true),
                "leaked {name}"
            );
            assert!(
                !allowed_environment_key(OsStr::new(name), false),
                "leaked {name}"
            );
        }
        assert!(allowed_environment_key(OsStr::new("PATH"), false));
        assert!(!allowed_environment_key(OsStr::new("Path"), false));
        assert!(!allowed_environment_key(OsStr::new("APPDATA"), false));
    }

    // Build a native fixture rather than requiring Node, Bash, or a real model
    // login. This exercises the same launcher on Windows, Linux, and macOS.
    fn native_fixture(root: &std::path::Path) -> std::path::PathBuf {
        let source = root.join("fixture.rs");
        std::fs::write(&source, r#"
            use std::io::Read;
            fn main() {
                let args: Vec<_> = std::env::args().skip(1).collect();
                if args.first().map(String::as_str) == Some("--exit") {
                    eprintln!("fixture failure");
                    std::process::exit(17);
                }
                if args.first().map(String::as_str) == Some("--wait") {
                    std::thread::sleep(std::time::Duration::from_secs(60));
                    return;
                }
                for arg in args { println!("ARG={arg:?}"); }
                let mut input = String::new();
                std::io::stdin().read_to_string(&mut input).expect("stdin");
                println!("STDIN={input:?}");
                println!("OVERLAY={}", std::env::var("SDK_LAUNCH_TEST").expect("explicit environment"));
                #[cfg(windows)]
                for key in ["SystemRoot", "USERPROFILE", "APPDATA", "LOCALAPPDATA"] {
                    assert!(std::env::var_os(key).is_some(), "missing {key}");
                }
            }
        "#).expect("write native fixture");
        let binary = root.join(if cfg!(windows) {
            "provider fixture.exe"
        } else {
            "provider fixture"
        });
        let result = std::process::Command::new("rustc")
            .args(["--edition=2021", "--crate-name=provider_fixture"])
            .arg(source)
            .arg("-o")
            .arg(&binary)
            .output()
            .expect("run rustc");
        assert!(
            result.status.success(),
            "fixture compilation: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        binary
    }

    async fn assert_launch(program: &std::path::Path, cwd: &std::path::Path) {
        let args = [
            "app-server",
            "--config",
            r#"{"mcpServers":{"test":{"url":"http://127.0.0.1:1234/mcp"}}}"#,
            "project with spaces",
            "héllo",
        ];
        let mut spec = CommandSpec::new(program);
        spec.args = args.iter().map(std::ffi::OsString::from).collect();
        // Exercise bare-name discovery with a per-command PATH, without
        // mutating the test process environment or borrowing installed CLIs.
        #[cfg(windows)]
        if program.components().count() == 1 {
            let inherited = std::env::var_os("PATH").unwrap_or_default();
            let paths = std::iter::once(cwd.to_path_buf()).chain(std::env::split_paths(&inherited));
            spec.environment
                .insert("Path".into(), std::env::join_paths(paths).unwrap());
        }
        spec.environment
            .insert("SDK_LAUNCH_TEST".into(), "explicit".into());
        let mut child = spawn(&spec, cwd).expect("launch provider fixture");
        let mut guard = ProcessTreeGuard::for_child(&child);
        let mut stdin = child.stdin.take().expect("piped stdin");
        stdin
            .write_all(b"{\"id\":1}\n")
            .await
            .expect("write protocol input");
        drop(stdin);
        let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
            .await
            .expect("provider deadline")
            .expect("provider output");
        guard.disarm();
        assert!(
            output.status.success(),
            "provider stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).expect("UTF-8 output");
        for arg in args {
            assert!(
                text.contains(&format!("ARG={arg:?}")),
                "argument changed: {arg:?}: {text}"
            );
        }
        assert!(text.contains(&format!("STDIN={:?}", "{\"id\":1}\n")));
        assert!(text.contains("OVERLAY=explicit"));
    }

    #[tokio::test]
    async fn native_provider_launch_preserves_arguments_stdin_and_exit_status() {
        let root = tempfile::Builder::new()
            .prefix("sdk provider space ")
            .tempdir()
            .expect("fixture directory");
        let binary = native_fixture(root.path());
        assert_launch(&binary, root.path()).await;

        // npm-installed provider CLIs on Windows are frequently .cmd shims.
        // Rust owns batch-file escaping; do not interpolate a shell command.
        #[cfg(windows)]
        for name in ["claude", "codex", "opencode"] {
            let shim = root.path().join(format!("{name}.cmd"));
            std::fs::write(
                &shim,
                format!("@echo off\r\n\"{}\" %*\r\n", binary.display()),
            )
            .expect("write cmd shim");
            assert_launch(&shim, root.path()).await;
            assert_launch(std::path::Path::new(name), root.path()).await;
        }

        let mut spec = CommandSpec::new(&binary);
        spec.args = vec!["--exit".into()];
        let child = spawn(&spec, root.path()).expect("failure fixture");
        let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
            .await
            .expect("exit deadline")
            .expect("failure output");
        assert_eq!(output.status.code(), Some(17));
        assert!(String::from_utf8_lossy(&output.stderr).contains("fixture failure"));

        spec.args = vec!["--wait".into()];
        let mut child = spawn(&spec, root.path()).expect("cancellation fixture");
        let mut guard = ProcessTreeGuard::for_child(&child);
        guard.terminate();
        let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .expect("termination deadline")
            .expect("termination status");
        assert!(!status.success());
    }
}
