//! Exercise SSH command streaming, interactive stdin, cancellation, and services.

use std::path::PathBuf;
use std::time::Duration;

use temps_agent_runtime::{
    CommandSpec, ExecutionTransport, ManagedProcessSpec, ManagedProcessSupervisor,
    SshHostKeyPolicy, SshTransport, TransportSpawnRequest,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn required(name: &str) -> std::result::Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|_| format!("{name} is required").into())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let host = required("SSH_TEST_HOST")?;
    let port = required("SSH_TEST_PORT")?.parse::<u16>()?;
    let user = required("SSH_TEST_USER")?;
    let identity = required("SSH_TEST_IDENTITY")?;
    let known_hosts = required("SSH_TEST_KNOWN_HOSTS")?;
    let workspace =
        PathBuf::from(std::env::var("SSH_TEST_WORKSPACE").unwrap_or_else(|_| "/workspace".into()));
    let transport = SshTransport::builder(host)
        .port(port)
        .user(user)
        .identity_file(identity)
        .known_hosts_file(known_hosts)
        .host_key_policy(SshHostKeyPolicy::Strict)
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    transport.validate_working_directory(&workspace).await?;

    let mut interactive = CommandSpec::new("/bin/sh");
    interactive.args.extend([
        "-c".into(),
        "printf 'approval-needed\\n'; read decision; printf 'approval-response=%s\\n' \"$decision\"; sleep 1; printf 'command-finished\\n'".into(),
    ]);
    interactive.interactive_stdin = true;
    let mut process = transport
        .spawn(TransportSpawnRequest {
            command: interactive,
            working_directory: workspace.clone(),
        })
        .await?;
    let mut stdin = process.take_stdin().ok_or("stdin missing")?;
    let mut stdout = process.take_stdout().ok_or("stdout missing")?;
    let mut stderr = process.take_stderr().ok_or("stderr missing")?;
    stdin.write_all(b"approved\n").await?;
    stdin.flush().await?;
    drop(stdin);
    let stdout_task = tokio::spawn(async move {
        let mut contents = String::new();
        stdout.read_to_string(&mut contents).await.map(|_| contents)
    });
    let stderr_task = tokio::spawn(async move {
        let mut contents = String::new();
        stderr.read_to_string(&mut contents).await.map(|_| contents)
    });
    let status = process.wait().await?;
    let output = stdout_task.await??;
    let errors = stderr_task.await??;
    assert!(status.success);
    assert!(output.contains("approval-response=approved"));
    println!("interactive_stdin=true");
    println!("command_stream={}", output.trim().replace('\n', "|"));
    println!("command_stderr={}", errors.trim().replace('\n', "|"));

    let mut cancel_command = CommandSpec::new("/bin/sh");
    cancel_command.args.extend(["-c".into(), "sleep 30".into()]);
    let mut cancelled = transport
        .spawn(TransportSpawnRequest {
            command: cancel_command,
            working_directory: workspace.clone(),
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    cancelled.terminate().await?;
    let cancelled_status = cancelled.wait().await?;
    assert!(!cancelled_status.success);
    println!("cancelled=true");

    let supervisor = ManagedProcessSupervisor::builder()
        .transport(transport)
        .build()?;
    let service = supervisor
        .start(
            ManagedProcessSpec::service("ssh-service", "/bin/sh", workspace)
                .args(["-c", "printf 'ssh-service-ready\\n'; sleep 30"]),
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let snapshot = service.snapshot().await?;
    let handle = snapshot
        .transport_handle
        .as_ref()
        .map_or("none", |value| value.native_id.as_str());
    let logs = service.logs().await?;
    assert!(logs
        .iter()
        .any(|line| line.text.contains("ssh-service-ready")));
    println!("service_handle={handle}");
    println!("service_streamed_log=true");
    println!("service_stopped={:?}", service.stop().await?.status);
    println!("transport=ssh");
    Ok(())
}
