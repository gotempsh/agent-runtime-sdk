//! Native-process fixtures: no provider account, network model, or real tools.
#![cfg(all(unix, feature = "claude"))]

use std::{fs, os::unix::fs::PermissionsExt, time::Duration};
use temps_agent_runtime::{
    lifecycle::{InvocationId, RuntimeFailure, RuntimeId},
    providers::Claude,
    retained::{InProcessRuntimeClient, RuntimeClient, RuntimeHandle, RuntimeSpec, TurnInput},
    AgentRuntime, PermissionMode, Provider, ProviderProcessRetention, TurnResult,
};

const SCRIPT: &str = r"#!/usr/bin/env python3
import json,os,sys,time
LOG=LOG_PATH
def log(kind, **data):
 with open(LOG,'a') as f:f.write(json.dumps(dict(kind=kind,pid=os.getpid(),**data))+'\n')
def emit(frame):
 print(json.dumps(frame),flush=True)
log('spawn')
for line in sys.stdin:
 frame=json.loads(line)
 if frame.get('type')=='control_request':
  log('probe')
  emit({'type':'control_response','response':{'request_id':frame['request_id'],'subtype':'success','response':{}}})
 elif frame.get('type')=='user':
  prompt=frame['message']['content'][0]['text'];log('prompt',prompt=prompt)
  if prompt=='crash':sys.exit(2)
  if prompt=='initial-hang':
   while True:time.sleep(1)
  emit({'type':'system','subtype':'init','session_id':'fixture-session'})
  if prompt=='chatter':
   while True:
    emit({'type':'unknown_noop'});time.sleep(.01)
  if prompt=='hang':
   while True:time.sleep(1)
  emit({'type':'result','subtype':'success','session_id':'fixture-session','result':'reply:'+prompt,'is_error':False,'num_turns':1,'total_cost_usd':0,'usage':{'input_tokens':1,'output_tokens':1}})
";

struct Fixture {
    _dir: tempfile::TempDir,
    log: std::path::PathBuf,
    client: InProcessRuntimeClient,
    handle: RuntimeHandle,
}
impl Fixture {
    async fn new(enabled: bool, idle_timeout: Duration) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let executable = dir.path().join("claude-fixture");
        let log = dir.path().join("events.jsonl");
        fs::write(
            &executable,
            SCRIPT.replace("LOG_PATH", &serde_json::to_string(&log).unwrap()),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let mut builder = AgentRuntime::builder();
        builder.register(Claude::with_executable(executable));
        if enabled {
            builder = builder.provider_process_retention(ProviderProcessRetention {
                max_processes: 1,
                idle_timeout,
                initialization_timeout: Duration::from_secs(10),
                active_inactivity_timeout: Some(Duration::from_secs(2)),
            });
        }
        let client = InProcessRuntimeClient::new(builder.build().unwrap());
        let handle = client
            .acquire(RuntimeSpec::new(
                RuntimeId::new("claude-native").unwrap(),
                Provider::Claude,
                dir.path(),
            ))
            .await
            .unwrap();
        Self {
            _dir: dir,
            log,
            client,
            handle,
        }
    }
    fn events(&self) -> Vec<serde_json::Value> {
        fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
    fn spawns(&self) -> Vec<i32> {
        self.events()
            .into_iter()
            .filter(|v| v["kind"] == "spawn")
            .map(|v| i32::try_from(v["pid"].as_i64().unwrap()).unwrap())
            .collect()
    }
    async fn turn(&self, id: &str, prompt: &str) -> Result<TurnResult, RuntimeFailure> {
        tokio::time::timeout(Duration::from_secs(15), async {
            self.handle
                .start_turn(TurnInput::new(InvocationId::new(id).unwrap(), prompt))
                .await
                .unwrap()
                .wait()
                .await
        })
        .await
        .expect("turn must be bounded")
    }
    async fn dispose(&self) {
        self.client
            .dispose(&RuntimeId::new("claude-native").unwrap())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn claude_reuses_first_session_and_health_checks_before_second_prompt() {
    let f = Fixture::new(true, Duration::from_secs(30)).await;
    assert_eq!(
        f.turn("one", "first").await.unwrap().session_id.as_deref(),
        Some("fixture-session")
    );
    assert_eq!(f.turn("two", "second").await.unwrap().text, "reply:second");
    assert_eq!(f.spawns().len(), 1, "{:?}", f.events());
    assert!(f.events().iter().any(|v| v["kind"] == "probe"));
    f.dispose().await;
}
#[tokio::test]
async fn claude_default_still_exits_each_turn() {
    let f = Fixture::new(false, Duration::from_secs(30)).await;
    assert!(!f.handle.driver_capabilities().retained_process);
    f.turn("one", "first").await.unwrap();
    f.turn("two", "second").await.unwrap();
    assert_eq!(f.spawns().len(), 2);
    f.dispose().await;
}
#[tokio::test]
async fn claude_permission_change_replaces_with_single_pool_slot() {
    let f = Fixture::new(true, Duration::from_secs(30)).await;
    f.turn("one", "first").await.unwrap();
    let mut input = TurnInput::new(InvocationId::new("changed").unwrap(), "changed");
    input.permission_mode = Some(PermissionMode::AcceptEdits);
    f.handle
        .start_turn(input)
        .await
        .unwrap()
        .wait()
        .await
        .unwrap();
    assert_eq!(f.spawns().len(), 2);
    f.dispose().await;
}
#[tokio::test]
async fn claude_idle_crash_recovers_before_delivery() {
    let f = Fixture::new(true, Duration::from_secs(30)).await;
    f.turn("one", "first").await.unwrap();
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(f.spawns()[0]),
        nix::sys::signal::Signal::SIGKILL,
    )
    .unwrap();
    assert_eq!(f.turn("two", "second").await.unwrap().text, "reply:second");
    assert_eq!(f.spawns().len(), 2);
    f.dispose().await;
}
#[tokio::test]
async fn claude_frozen_idle_process_is_replaced_without_duplicate_prompt() {
    let f = Fixture::new(true, Duration::from_secs(30)).await;
    f.turn("one", "first").await.unwrap();
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(f.spawns()[0]),
        nix::sys::signal::Signal::SIGSTOP,
    )
    .unwrap();
    assert_eq!(f.turn("two", "second").await.unwrap().text, "reply:second");
    assert_eq!(
        f.events()
            .iter()
            .filter(|v| v["prompt"] == "second")
            .count(),
        1
    );
    f.dispose().await;
}
#[tokio::test]
async fn claude_active_failure_is_not_replayed_and_next_turn_recovers() {
    let f = Fixture::new(true, Duration::from_secs(30)).await;
    assert!(f.turn("one", "crash").await.is_err());
    assert_eq!(f.turn("two", "second").await.unwrap().text, "reply:second");
    assert_eq!(
        f.events().iter().filter(|v| v["prompt"] == "crash").count(),
        1
    );
    f.dispose().await;
}
#[tokio::test]
async fn claude_active_silence_is_bounded_and_runtime_is_reusable() {
    let f = Fixture::new(true, Duration::from_secs(30)).await;
    assert!(f.turn("one", "hang").await.is_err());
    assert_eq!(f.turn("two", "second").await.unwrap().text, "reply:second");
    f.dispose().await;
}
#[tokio::test]
async fn claude_idle_expiry_resumes_in_a_new_process() {
    let f = Fixture::new(true, Duration::from_millis(50)).await;
    f.turn("one", "first").await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        f.turn("two", "second").await.unwrap().session_id.as_deref(),
        Some("fixture-session")
    );
    assert_eq!(f.spawns().len(), 2);
    f.dispose().await;
}

#[tokio::test]
async fn claude_initial_silence_is_bounded_without_replaying_prompt() {
    let f = Fixture::new(true, Duration::from_secs(30)).await;
    assert!(f.turn("one", "initial-hang").await.is_err());
    assert_eq!(
        f.events()
            .iter()
            .filter(|v| v["prompt"] == "initial-hang")
            .count(),
        1
    );
    assert_eq!(f.turn("two", "second").await.unwrap().text, "reply:second");
    f.dispose().await;
}

#[tokio::test]
async fn claude_noop_frames_do_not_hide_a_stalled_turn() {
    let f = Fixture::new(true, Duration::from_secs(30)).await;
    assert!(f.turn("one", "chatter").await.is_err());
    assert_eq!(f.turn("two", "second").await.unwrap().text, "reply:second");
    f.dispose().await;
}
