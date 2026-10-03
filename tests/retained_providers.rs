//! Native-process fixtures: no provider account, network model, or real tools.
#![cfg(all(unix, feature = "claude"))]

use std::{fs, os::unix::fs::PermissionsExt, time::Duration};
use temps_agent_runtime::{
    lifecycle::{InvocationId, RuntimeFailure, RuntimeFailureKind, RuntimeId},
    providers::Claude,
    retained::{
        CompactionInput, InProcessRuntimeClient, RuntimeClient, RuntimeEvent, RuntimeHandle,
        RuntimeSpec, TurnHandle, TurnInput,
    },
    AgentRuntime, AgentTaskActivityKind, PermissionMode, Provider, ProviderProcessRetention,
    TurnEvent, TurnResult,
};
use tokio::task::JoinHandle;

const SCRIPT: &str = r"#!/usr/bin/env python3
import json,os,sys,threading,time
LOG=LOG_PATH
RELEASE=RELEASE_PATH
LOCK=threading.Lock()
def log(kind, **data):
 with open(LOG,'a') as f:f.write(json.dumps(dict(kind=kind,pid=os.getpid(),**data))+'\n')
def emit(frame):
 with LOCK:print(json.dumps(frame),flush=True)
def result(text):
 emit({'type':'result','subtype':'success','session_id':'fixture-session','result':text,'is_error':False,'num_turns':1,'total_cost_usd':0,'usage':{'input_tokens':1,'output_tokens':1}})
def subagent(follow_up):
 # A background subagent, as Claude reports one: progress and nested tool
 # calls until it finishes, then a notification that wakes the parent agent.
 log('bg-start')
 while not os.path.exists(RELEASE):
  emit({'type':'system','subtype':'task_progress','task_id':'bg-1','description':'Waiting'})
  emit({'type':'assistant','parent_tool_use_id':'toolu_bg','message':{'content':[{'type':'tool_use','id':'toolu_poll','name':'Bash','input':{'command':'true'}}]}})
  time.sleep(.02)
 emit({'type':'system','subtype':'background_tasks_changed','tasks':[]})
 emit({'type':'system','subtype':'task_notification','task_id':'bg-1','status':'completed','summary':'background done'})
 log('bg-finished')
 if follow_up:
  emit({'type':'system','subtype':'init','session_id':'fixture-session'})
  emit({'type':'assistant','parent_tool_use_id':None,'message':{'content':[{'type':'text','text':'FOLLOWUP'}]}})
  result('FOLLOWUP')
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
  if prompt=='compact-crash':
   emit({'type':'system','subtype':'status','status':'compacting','session_id':'fixture-session'});sys.exit(2)
  if prompt=='chatter':
   while True:
    emit({'type':'unknown_noop'});time.sleep(.01)
  if prompt=='hang':
   while True:time.sleep(1)
  if prompt in('spawn-bg','spawn-bg-silent'):
   emit({'type':'system','subtype':'task_started','task_id':'bg-1','tool_use_id':'toolu_bg','description':'Background work','subagent_type':'general-purpose','task_type':'local_agent','is_backgrounded':True})
   emit({'type':'system','subtype':'background_tasks_changed','tasks':[{'task_id':'bg-1','task_type':'local_agent','description':'Background work'}]})
   emit({'type':'assistant','parent_tool_use_id':None,'message':{'content':[{'type':'text','text':'LAUNCHED'}]}})
   result('LAUNCHED')
   threading.Thread(target=subagent,args=(prompt=='spawn-bg',),daemon=True).start()
   continue
  result('reply:'+prompt)
";

struct Fixture {
    _dir: tempfile::TempDir,
    log: std::path::PathBuf,
    release: std::path::PathBuf,
    client: InProcessRuntimeClient,
    handle: RuntimeHandle,
}

/// A started turn whose events are drained concurrently, as an application
/// does; an unread event stream would apply backpressure to the provider.
struct Running {
    events: JoinHandle<Vec<TurnEvent>>,
    completion: JoinHandle<Result<TurnResult, RuntimeFailure>>,
}
impl Running {
    fn new(turn: TurnHandle) -> Self {
        let (mut stream, completion) = turn.into_parts();
        Self {
            events: tokio::spawn(async move {
                let mut events = Vec::new();
                while let Some(envelope) = stream.next().await {
                    if let RuntimeEvent::ProviderEvent { event } = envelope.event {
                        events.push(event);
                    }
                }
                events
            }),
            completion: tokio::spawn(completion.wait()),
        }
    }
    async fn finish(self) -> (Vec<TurnEvent>, Result<TurnResult, RuntimeFailure>) {
        tokio::time::timeout(Duration::from_secs(15), async {
            let result = self.completion.await.unwrap();
            (self.events.await.unwrap(), result)
        })
        .await
        .expect("turn must be bounded")
    }
}
impl Fixture {
    async fn new(enabled: bool, idle_timeout: Duration) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let executable = dir.path().join("claude-fixture");
        let log = dir.path().join("events.jsonl");
        let release = dir.path().join("release");
        fs::write(
            &executable,
            SCRIPT
                .replace("LOG_PATH", &serde_json::to_string(&log).unwrap())
                .replace("RELEASE_PATH", &serde_json::to_string(&release).unwrap()),
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
            release,
            client,
            handle,
        }
    }
    fn input(id: &str, prompt: &str) -> TurnInput {
        TurnInput::new(InvocationId::new(id).unwrap(), prompt)
    }
    /// Let the fixture's background subagent finish.
    fn release_background(&self) {
        fs::write(&self.release, b"done").unwrap();
    }
    fn logged(&self, kind: &str) -> usize {
        self.events().iter().filter(|v| v["kind"] == kind).count()
    }
    async fn wait_for_log(&self, kind: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.logged(kind) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("fixture never logged {kind}: {:?}", self.events()));
    }
    /// Start a turn, retrying briefly while the runtime reports busy, as an
    /// application following `RetryAdvice::After` does.
    async fn start_retrying(&self, id: &str, prompt: &str) -> TurnHandle {
        for _ in 0..100 {
            match self.handle.start_turn(Self::input(id, prompt)).await {
                Ok(turn) => return turn,
                Err(failure) if failure.kind == RuntimeFailureKind::RuntimeBusy => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(failure) => panic!("turn {id} failed to start: {failure:?}"),
            }
        }
        panic!("runtime stayed busy for turn {id}");
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
    async fn compaction_events(
        &self,
        turn: TurnHandle,
    ) -> (Vec<TurnEvent>, Result<TurnResult, RuntimeFailure>) {
        tokio::time::timeout(Duration::from_secs(15), async {
            let (mut stream, completion) = turn.into_parts();
            let mut observed = Vec::new();
            while let Some(envelope) = stream.next().await {
                if let RuntimeEvent::ProviderEvent { event } = envelope.event {
                    if matches!(
                        event,
                        TurnEvent::CompactionStarted { .. }
                            | TurnEvent::CompactionCompleted { .. }
                            | TurnEvent::CompactionFailed { .. }
                    ) {
                        observed.push(event);
                    }
                }
            }
            (observed, completion.wait().await)
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
async fn claude_permission_change_is_switched_in_place() {
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
    assert_eq!(f.spawns().len(), 1);
    f.dispose().await;
}
#[tokio::test]
async fn claude_change_needing_a_new_process_replaces_with_single_pool_slot() {
    let f = Fixture::new(true, Duration::from_secs(30)).await;
    f.turn("one", "first").await.unwrap();
    let mut input = TurnInput::new(InvocationId::new("changed").unwrap(), "changed");
    input.reasoning = Some("off".into());
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

#[tokio::test]
async fn claude_exiting_mid_compaction_never_leaves_it_open() {
    for retained in [false, true] {
        let f = Fixture::new(retained, Duration::from_secs(30)).await;
        let turn = f
            .handle
            .start_turn(TurnInput::new(
                InvocationId::new("compact-crash").unwrap(),
                "compact-crash",
            ))
            .await
            .unwrap();
        let (events, result) = f.compaction_events(turn).await;
        assert!(result.is_err());
        assert!(
            matches!(
                events.as_slice(),
                [
                    TurnEvent::CompactionStarted { .. },
                    TurnEvent::CompactionFailed {
                        message: Some(_),
                        ..
                    }
                ]
            ),
            "retained={retained}: {events:?}"
        );
        f.dispose().await;
    }
}

#[tokio::test]
async fn an_unconfirmed_manual_compaction_is_closed_as_failed() {
    // The fixture answers `/compact` with an ordinary success result and no
    // compact boundary, as Claude does when it declines to compact.
    let f = Fixture::new(false, Duration::from_secs(30)).await;
    f.turn("seed", "hello").await.unwrap();
    let turn = f
        .handle
        .compact(CompactionInput::new(InvocationId::new("compact").unwrap()))
        .await
        .unwrap();
    let (events, result) = f.compaction_events(turn).await;
    assert!(result.is_ok());
    assert!(
        matches!(
            events.as_slice(),
            [
                TurnEvent::CompactionStarted { .. },
                TurnEvent::CompactionFailed {
                    message: Some(message),
                    ..
                }
            ] if message.contains("without confirming")
        ),
        "{events:?}"
    );
    f.dispose().await;
}

#[tokio::test]
async fn claude_background_subagent_survives_a_new_turn() {
    let f = Fixture::new(true, Duration::from_secs(30)).await;
    let first = Running::new(
        f.handle
            .start_turn(Fixture::input("one", "spawn-bg"))
            .await
            .unwrap(),
    );
    f.wait_for_log("bg-start").await;

    // The first turn has answered and only its background subagent is still
    // working, so a new prompt takes over the live process instead of
    // requiring that turn to be interrupted.
    let second = Running::new(f.start_retrying("two", "second").await);
    let (_, first_result) = first.finish().await;
    assert_eq!(first_result.unwrap().text, "LAUNCHED");
    f.wait_for_log("prompt").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !second.completion.is_finished(),
        "inherited background work keeps the new turn open"
    );
    assert_eq!(f.logged("bg-finished"), 0, "the subagent is still running");

    f.release_background();
    let (events, second_result) = second.finish().await;
    let text = second_result.unwrap().text;
    assert!(text.contains("reply:second"), "{text}");
    assert!(
        text.contains("FOLLOWUP"),
        "the parent's answer to the notification reaches the turn: {text}"
    );
    assert!(events.iter().any(|event| matches!(
        event,
        TurnEvent::ToolCall { task_id, .. } if task_id.as_deref() == Some("bg-1")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        TurnEvent::TaskActivity { activity }
            if activity.task_id == "bg-1" && activity.kind == AgentTaskActivityKind::Completed
    )));
    assert_eq!(f.logged("bg-finished"), 1);
    assert_eq!(
        f.events()
            .iter()
            .filter(|v| v["prompt"] == "second")
            .count(),
        1
    );
    assert_eq!(
        f.logged("probe"),
        0,
        "a handed-off process is not probed while background output is queued"
    );

    assert_eq!(f.turn("three", "third").await.unwrap().text, "reply:third");
    assert_eq!(f.spawns().len(), 1, "{:?}", f.events());
    f.dispose().await;
}

#[tokio::test]
async fn claude_follow_up_after_background_work_belongs_to_its_turn() {
    let f = Fixture::new(true, Duration::from_secs(30)).await;
    let turn = Running::new(
        f.handle
            .start_turn(Fixture::input("one", "spawn-bg"))
            .await
            .unwrap(),
    );
    f.wait_for_log("bg-start").await;
    f.release_background();
    let (_, result) = turn.finish().await;
    let text = result.unwrap().text;
    assert!(
        text.contains("LAUNCHED") && text.contains("FOLLOWUP"),
        "{text}"
    );
    // The follow-up was read by its turn rather than by the idle supervisor,
    // which would have retired the process as unassignable output.
    assert_eq!(f.turn("two", "second").await.unwrap().text, "reply:second");
    assert_eq!(f.spawns().len(), 1, "{:?}", f.events());
    f.dispose().await;
}

#[tokio::test]
async fn claude_unanswered_background_drain_completes_after_a_quiet_grace() {
    let f = Fixture::new(true, Duration::from_secs(30)).await;
    let turn = Running::new(
        f.handle
            .start_turn(Fixture::input("one", "spawn-bg-silent"))
            .await
            .unwrap(),
    );
    f.wait_for_log("bg-start").await;
    f.release_background();
    let (_, result) = turn.finish().await;
    assert_eq!(result.unwrap().text, "LAUNCHED");
    assert_eq!(f.turn("two", "second").await.unwrap().text, "reply:second");
    assert_eq!(f.spawns().len(), 1, "{:?}", f.events());
    f.dispose().await;
}

#[tokio::test]
async fn claude_turn_still_answering_keeps_rejecting_overlap() {
    let f = Fixture::new(true, Duration::from_secs(30)).await;
    let first = f
        .handle
        .start_turn(Fixture::input("one", "hang"))
        .await
        .unwrap();
    f.wait_for_log("prompt").await;
    let error = f
        .handle
        .start_turn(Fixture::input("two", "second"))
        .await
        .expect_err("a turn that has not answered cannot hand off");
    assert_eq!(error.kind, RuntimeFailureKind::RuntimeBusy);
    first.interrupt().await;
    assert!(f.events().iter().all(|v| v["prompt"] != "second"));
    f.dispose().await;
}

#[tokio::test]
async fn claude_background_work_without_retention_keeps_rejecting_overlap() {
    let f = Fixture::new(false, Duration::from_secs(30)).await;
    let first = Running::new(
        f.handle
            .start_turn(Fixture::input("one", "spawn-bg"))
            .await
            .unwrap(),
    );
    f.wait_for_log("bg-start").await;
    let error = f
        .handle
        .start_turn(Fixture::input("two", "second"))
        .await
        .expect_err("a one-shot process exits with its turn and cannot be handed off");
    assert_eq!(error.kind, RuntimeFailureKind::RuntimeBusy);
    f.release_background();
    let (_, result) = first.finish().await;
    // One-shot behavior is unchanged; whether this fixture prints its
    // follow-up before stdin closes is a race outside this test's scope.
    assert!(result.unwrap().text.starts_with("LAUNCHED"));
    f.dispose().await;
}
