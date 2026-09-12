//! Exercise command, streaming, reattach, cancellation, service, and approval behavior.

use std::path::{Path, PathBuf};
use std::time::Duration;

use temps_agent_runtime::{
    AgentRuntime, CommandSpec, ExecutionTransport, ManagedProcessSpec, ManagedProcessSupervisor,
    NoopEventSink, PermissionMode, Provider, RuntimeError, SecretString, TempsSandboxAuth,
    TempsSandboxTransport, TransportErrorKind, TransportSpawnRequest, TurnRequest,
};
use tokio::io::AsyncReadExt;

fn required(name: &str) -> std::result::Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|_| format!("{name} is required").into())
}

async fn capture(
    transport: &TempsSandboxTransport,
    workspace: &Path,
    script: &str,
) -> std::result::Result<(String, String), Box<dyn std::error::Error>> {
    let mut command = CommandSpec::new("/bin/sh");
    command.args.extend(["-c".into(), script.into()]);
    let mut process = transport
        .spawn(TransportSpawnRequest {
            command,
            working_directory: workspace.to_path_buf(),
        })
        .await?;
    drop(process.take_stdin());
    let mut stdout = process.take_stdout().ok_or("stdout missing")?;
    let mut stderr = process.take_stderr().ok_or("stderr missing")?;
    let stdout_task = tokio::spawn(async move {
        let mut contents = String::new();
        stdout.read_to_string(&mut contents).await.map(|_| contents)
    });
    let stderr_task = tokio::spawn(async move {
        let mut contents = String::new();
        stderr.read_to_string(&mut contents).await.map(|_| contents)
    });
    let handle = process.handle().native_id.clone();
    let status = process.wait().await?;
    if !status.success {
        return Err(format!("sandbox job {handle} exited with {:?}", status.code).into());
    }
    Ok((stdout_task.await??, stderr_task.await??))
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let base_url = required("TEMPS_TEST_BASE_URL")?;
    let sandbox_id = required("TEMPS_TEST_SANDBOX_ID")?;
    let auth = if let Ok(token) = std::env::var("TEMPS_TEST_API_KEY") {
        TempsSandboxAuth::Bearer(SecretString::new(token))
    } else {
        TempsSandboxAuth::SessionCookie(SecretString::new(required("TEMPS_TEST_SESSION_COOKIE")?))
    };
    let transport = TempsSandboxTransport::builder(base_url, &sandbox_id, auth)
        .poll_interval(Duration::from_millis(75))
        .build()?;
    let workspace = PathBuf::from(
        std::env::var("TEMPS_TEST_WORKSPACE")
            .unwrap_or_else(|_| "/home/temps/workspace".to_string()),
    );

    transport.validate_working_directory(&workspace).await?;
    let missing = transport
        .validate_working_directory(Path::new("/workspace/does-not-exist"))
        .await
        .expect_err("missing directory must be typed");
    assert_eq!(missing.kind, TransportErrorKind::WorkingDirectoryNotFound);
    println!("typed_missing_directory={:?}", missing.kind);

    let (stdout, stderr) = capture(
        &transport,
        &workspace,
        "printf 'sandbox-stream-one\\n'; sleep 1; printf 'sandbox-stream-two\\n'",
    )
    .await?;
    println!("command_stream={}", stdout.trim().replace('\n', "|"));
    println!("command_stderr={}", stderr.trim().replace('\n', "|"));

    let mut reattach_command = CommandSpec::new("/bin/sh");
    reattach_command.args.extend([
        "-c".into(),
        "printf 'reattach-one\\n'; sleep 1; printf 'reattach-two\\n'".into(),
    ]);
    let mut original = transport
        .spawn(TransportSpawnRequest {
            command: reattach_command,
            working_directory: workspace.clone(),
        })
        .await?;
    let native_handle = original.handle().clone();
    drop(original.take_stdin());
    let mut original_stdout = original.take_stdout().ok_or("original stdout missing")?;
    let original_drain = tokio::spawn(async move {
        let mut contents = Vec::new();
        original_stdout.read_to_end(&mut contents).await
    });
    let mut attached = transport.attach(&native_handle, Some(0)).await?;
    drop(attached.take_stdin());
    let mut attached_stdout = attached.take_stdout().ok_or("attached stdout missing")?;
    let mut attached_output = String::new();
    attached_stdout.read_to_string(&mut attached_output).await?;
    let attached_status = attached.wait().await?;
    let original_status = original.wait().await?;
    original_drain.await??;
    assert!(attached_status.success && original_status.success);
    println!("reattach_handle={}", native_handle.native_id);
    println!(
        "reattach_stream={}",
        attached_output.trim().replace('\n', "|")
    );

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
        .transport(transport.clone())
        .build()?;
    let service = supervisor
        .start(
            ManagedProcessSpec::service("sandbox-service", "/bin/sh", &workspace)
                .args(["-c", "printf 'service-ready\\n'; sleep 30"]),
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let service_snapshot = service.snapshot().await?;
    let service_handle = service_snapshot
        .transport_handle
        .as_ref()
        .map_or("none", |handle| handle.native_id.as_str());
    let service_logs = service.logs().await?;
    assert!(service_logs
        .iter()
        .any(|line| line.text.contains("service-ready")));
    println!("service_handle={service_handle}");
    println!("service_streamed_log=true");
    let stopped = service.stop().await?;
    println!("service_stopped={:?}", stopped.status);

    let runtime = AgentRuntime::builder().transport(transport).build()?;
    let mut approval_request = TurnRequest::new(
        Provider::Claude,
        workspace,
        "Approval capability probe; this must not start Claude.",
    );
    approval_request.permission_mode = PermissionMode::Plan;
    let approval_error = runtime
        .run(approval_request, &NoopEventSink, None)
        .await
        .expect_err("HTTP exec transport cannot support live approvals");
    match approval_error {
        RuntimeError::TransportCapabilityUnavailable { capability, .. } => {
            assert_eq!(capability, "interactive_stdin");
            println!("approvals=typed_unsupported:{capability}");
        }
        other => return Err(format!("unexpected approval error: {other}").into()),
    }

    println!("transport=temps_sandbox");
    println!("sandbox_id={sandbox_id}");
    Ok(())
}
