//! `DockerTransport` against a real Docker daemon.
//!
//! Opt-in: set `TEMPS_AGENT_RUNTIME_DOCKER_TESTS=1` (and optionally
//! `TEMPS_AGENT_RUNTIME_DOCKER_TEST_IMAGE`, default `ubuntu:24.04`, which must
//! provide Bash). Each test starts a throwaway container and removes
//! it afterwards.
#![cfg(feature = "docker")]

use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use temps_agent_runtime::{
    CommandSpec, DockerTransport, ExecutionTransport, Provider, TransportReadinessRequest,
    TransportSpawnRequest,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Container(String);

impl Container {
    async fn start() -> Option<Self> {
        Self::start_with(&[]).await
    }

    async fn start_with(options: &[&str]) -> Option<Self> {
        if std::env::var("TEMPS_AGENT_RUNTIME_DOCKER_TESTS").as_deref() != Ok("1") {
            return None;
        }
        let image = std::env::var("TEMPS_AGENT_RUNTIME_DOCKER_TEST_IMAGE")
            .unwrap_or_else(|_| "ubuntu:24.04".into());
        let output = tokio::process::Command::new("docker")
            .args(["run", "-d", "--rm", "--init"])
            .args(options)
            .args([image.as_str(), "sleep", "300"])
            .output()
            .await
            .expect("docker run");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Some(Self(
            String::from_utf8(output.stdout).unwrap().trim().to_string(),
        ))
    }

    fn transport(&self) -> DockerTransport {
        DockerTransport::builder(&self.0).build().unwrap()
    }

    async fn exec(&self, script: &str) -> String {
        let output = tokio::process::Command::new("docker")
            .args(["exec", &self.0, "bash", "-c", script])
            .output()
            .await
            .unwrap();
        String::from_utf8_lossy(&output.stdout).into_owned()
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.0])
            .output();
    }
}

fn request(program: &str, args: &[&str]) -> TransportSpawnRequest {
    let mut command = CommandSpec::new(program);
    command.args = args.iter().map(OsString::from).collect();
    TransportSpawnRequest {
        command,
        working_directory: PathBuf::from("/tmp"),
    }
}

#[tokio::test]
async fn runs_a_cli_in_the_container_with_its_environment() {
    let Some(container) = Container::start().await else {
        return;
    };
    let transport = container.transport();
    assert!(transport.capabilities().interactive_stdin);

    let readiness = transport
        .readiness(TransportReadinessRequest {
            provider: Provider::Claude,
            program: PathBuf::from("bash"),
        })
        .await
        .unwrap();
    assert!(readiness.installed, "{readiness:?}");
    transport
        .validate_working_directory(&PathBuf::from("/tmp"))
        .await
        .unwrap();
    assert!(transport
        .validate_working_directory(&PathBuf::from("/does/not/exist"))
        .await
        .is_err());

    let mut spawn = request(
        "bash",
        &[
            "-c",
            "read line; printf '%s|%s|%s' \"$line\" \"$SECRET\" \"$PWD\"",
        ],
    );
    spawn
        .command
        .environment
        .insert("SECRET".into(), "s3cret value".into());
    let mut process = transport.spawn(spawn).await.unwrap();
    let mut stdin = process.take_stdin().unwrap();
    stdin.write_all(b"hello from stdin\n").await.unwrap();
    drop(stdin);
    let mut output = String::new();
    process
        .take_stdout()
        .unwrap()
        .read_to_string(&mut output)
        .await
        .unwrap();
    assert_eq!(output, "hello from stdin|s3cret value|/tmp");
    assert!(process.wait().await.unwrap().success);
    // The value travelled in the private launcher, not on any command line.
    assert!(!container
        .exec("cat /proc/*/cmdline 2>/dev/null | tr '\\0' ' '")
        .await
        .contains("s3cret"));
}

#[tokio::test]
async fn terminate_stops_the_process_group_inside_the_container() {
    let Some(container) = Container::start().await else {
        return;
    };
    let transport = container.transport();
    let mut process = transport
        .spawn(request("bash", &["-c", "sleep 600 & sleep 600; wait"]))
        .await
        .unwrap();
    for _ in 0..50 {
        if container.exec("pgrep -c -x sleep").await.trim() == "3" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // `sleep 300` keeps the container alive; the turn adds two more.
    assert_eq!(container.exec("pgrep -c -x sleep").await.trim(), "3");
    process.terminate().await.unwrap();
    assert_eq!(container.exec("pgrep -c -x sleep").await.trim(), "1");
}

#[tokio::test]
async fn a_selected_user_gets_its_own_home() {
    let Some(container) = Container::start_with(&["--env", "HOME=/root"]).await else {
        return;
    };
    // ubuntu:24.04 ships the `ubuntu` user (home /home/ubuntu).
    let transport = DockerTransport::builder(&container.0)
        .user("ubuntu")
        .build()
        .unwrap();
    let mut process = transport
        .spawn(request(
            "sh",
            &["-c", "printf '%s|%s' \"$HOME\" \"$(id -un)\""],
        ))
        .await
        .unwrap();
    let mut output = String::new();
    process
        .take_stdout()
        .unwrap()
        .read_to_string(&mut output)
        .await
        .unwrap();
    assert!(process.wait().await.unwrap().success);
    assert_eq!(output, "/home/ubuntu|ubuntu");
}

#[tokio::test]
async fn loopback_ports_are_refused_clearly() {
    let Some(container) = Container::start().await else {
        return;
    };
    let mut spawn = request("true", &[]);
    spawn.command.loopback_ports.push(4100);
    let Err(error) = container.transport().spawn(spawn).await else {
        panic!("loopback ports are not forwarded into containers");
    };
    assert_eq!(
        error.kind,
        temps_agent_runtime::TransportErrorKind::Unsupported
    );
}
