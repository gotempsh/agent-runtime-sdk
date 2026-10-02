//! Native-process fixtures for messages sent into running Claude turns and
//! cooperative interruption: no provider account, network model, or tools.
//!
//! The fixture models the parts of Claude Code's streaming protocol the SDK
//! relies on: a command queue that keeps reading stdin while it works,
//! `command_lifecycle` frames keyed by each message's UUID, messages that
//! arrive mid-exchange folded into the running reply, a native `interrupt`
//! that cancels only foreground work, and background subagents whose
//! completion wakes the parent agent.
#![cfg(all(unix, feature = "claude"))]

use std::{fs, os::unix::fs::PermissionsExt, sync::Arc, time::Duration};

use async_trait::async_trait;
use temps_agent_runtime::{
    lifecycle::{InterruptOutcome, InvocationId, RuntimeFailure, RuntimeFailureKind, RuntimeId},
    providers::Claude,
    retained::{
        InProcessRuntimeClient, RuntimeClient, RuntimeEvent, RuntimeHandle, RuntimeSpec,
        TurnHandle, TurnInput,
    },
    AgentRuntime, AgentTaskActivityKind, ApprovalDecision, ApprovalRequest, InteractionHandler,
    Provider, ProviderProcessRetention, QuestionAnswer, QuestionRequest, TurnEvent, TurnResult,
};
use tokio::task::JoinHandle;

const SCRIPT: &str = r"#!/usr/bin/env python3
import collections,json,os,sys,threading,time
LOG=LOG_PATH
DIR=DIR_PATH
LOCK=threading.Lock()
WORK=threading.Condition()
QUEUE=collections.deque()
INTERRUPT=threading.Event()
RUNNING=threading.Event()
RESPONSES={}
def log(kind, **data):
 with open(LOG,'a') as f:f.write(json.dumps(dict(kind=kind,pid=os.getpid(),**data))+'\n')
def emit(frame):
 with LOCK:print(json.dumps(frame),flush=True)
def lifecycle(uuid,state):
 if uuid:emit({'type':'command_lifecycle','command_uuid':uuid,'state':state})
def released(name):
 return os.path.exists(os.path.join(DIR,name))
def result(text,error=False):
 emit({'type':'result','subtype':'error_during_execution' if error else 'success','is_error':error,'session_id':'fixture-session','result':'' if error else text,'usage':{'input_tokens':1,'output_tokens':1}})
def take_queued():
 with WORK:
  items=list(QUEUE);QUEUE.clear();return items
def subagent(approval):
 log('bg-start')
 if approval:
  while not released('approval-trigger'):time.sleep(.02)
  emit({'type':'control_request','request_id':'perm-bg','request':{'subtype':'can_use_tool','tool_name':'Bash','input':{'command':'make'}}})
  while 'perm-bg' not in RESPONSES:time.sleep(.02)
  log('approval',behavior=RESPONSES['perm-bg'].get('behavior'))
 while not released('bg-release'):
  emit({'type':'system','subtype':'task_progress','task_id':'bg-1','description':'Waiting'})
  time.sleep(.05)
 emit({'type':'system','subtype':'background_tasks_changed','tasks':[]})
 emit({'type':'system','subtype':'task_notification','task_id':'bg-1','status':'completed','summary':'background done'})
 log('bg-finished')
 # The notification wakes the parent agent, which answers it on its own.
 with WORK:
  QUEUE.append(('internal-follow-up','__follow_up__'));WORK.notify()
def start_background(approval=False):
 emit({'type':'system','subtype':'task_started','task_id':'bg-1','tool_use_id':'toolu_bg','description':'Background work','subagent_type':'general-purpose','task_type':'local_agent','is_backgrounded':True})
 emit({'type':'system','subtype':'background_tasks_changed','tasks':[{'task_id':'bg-1','task_type':'local_agent','description':'Background work'}]})
 threading.Thread(target=subagent,args=(approval,),daemon=True).start()
def foreground(uuid):
 # A foreground command: it runs until released or interrupted. Messages
 # that arrive meanwhile are folded into this exchange's reply.
 log('work-start')
 emit({'type':'assistant','parent_tool_use_id':None,'message':{'content':[{'type':'tool_use','id':'toolu_fg','name':'Bash','input':{'command':'build'}}]}})
 emit({'type':'system','subtype':'task_started','task_id':'fg-1','tool_use_id':'toolu_fg','description':'Build','task_type':'local_bash','is_backgrounded':False})
 while not released('fg-release'):
  if INTERRUPT.is_set():
   log('work-stopped')
   emit({'type':'system','subtype':'task_notification','task_id':'fg-1','status':'stopped'})
   result('',error=True);lifecycle(uuid,'cancelled')
   # Like Claude, the stopped exchange reaches the transcript just after
   # the turn is reported over.
   time.sleep(.05)
   emit({'type':'user','message':{'role':'user','content':[{'type':'tool_result','tool_use_id':'toolu_fg','is_error':True,'content':'Tool use rejected.'}]}})
   emit({'type':'user','message':{'role':'user','content':[{'type':'text','text':'[Request interrupted by user for tool use]'}]}})
   return
  time.sleep(.02)
 log('work-finished')
 emit({'type':'system','subtype':'task_notification','task_id':'fg-1','status':'completed'})
 folded=take_queued()
 for extra,_ in folded:lifecycle(extra,'started')
 text='built'+''.join(' and reply:'+prompt for _,prompt in folded)
 emit({'type':'assistant','parent_tool_use_id':None,'message':{'content':[{'type':'text','text':text}]}})
 result(text)
 for extra,_ in folded:lifecycle(extra,'completed')
 lifecycle(uuid,'completed')
def run(uuid,prompt):
 lifecycle(uuid,'started')
 emit({'type':'system','subtype':'init','session_id':'fixture-session'})
 if prompt=='__follow_up__':
  emit({'type':'assistant','parent_tool_use_id':None,'message':{'content':[{'type':'text','text':'FOLLOWUP'}]}})
  result('FOLLOWUP');return
 log('prompt',prompt=prompt)
 if INTERRUPT.is_set():
  # Claude runs messages queued behind an interrupted exchange; the SDK
  # stops each one as it starts.
  time.sleep(.2)
 if INTERRUPT.is_set():
  log('cancelled',prompt=prompt)
  result('',error=True);lifecycle(uuid,'cancelled');return
 if prompt in('spawn-bg','spawn-bg-approval'):
  start_background(approval=prompt=='spawn-bg-approval')
  emit({'type':'assistant','parent_tool_use_id':None,'message':{'content':[{'type':'text','text':'LAUNCHED'}]}})
  result('LAUNCHED');lifecycle(uuid,'completed');return
 if prompt=='spawn-bg-then-work':
  start_background()
  foreground(uuid);return
 if prompt=='work':
  foreground(uuid);return
 result('reply:'+prompt);lifecycle(uuid,'completed')
def worker():
 while True:
  with WORK:
   while not QUEUE:WORK.wait()
   uuid,prompt=QUEUE.popleft()
  RUNNING.set()
  run(uuid,prompt)
  RUNNING.clear()
  # An interrupt applies to the exchange that was running when it arrived.
  if not QUEUE:INTERRUPT.clear()
log('spawn')
threading.Thread(target=worker,daemon=True).start()
for line in sys.stdin:
 frame=json.loads(line)
 kind=frame.get('type')
 if kind=='control_request':
  subtype=frame['request'].get('subtype')
  log('control',subtype=subtype)
  if subtype=='interrupt':
   # Like Claude, an interrupt with nothing running is a no-op.
   if RUNNING.is_set():INTERRUPT.set()
   with WORK:queued=[uuid for uuid,_ in QUEUE]
   emit({'type':'control_response','response':{'request_id':frame['request_id'],'subtype':'success','response':{'still_queued':queued}}})
  else:
   emit({'type':'control_response','response':{'request_id':frame['request_id'],'subtype':'success','response':{}}})
 elif kind=='control_response':
  response=frame['response']
  RESPONSES[response['request_id']]=response.get('response',{})
 elif kind=='user':
  uuid=frame.get('uuid');text=frame['message']['content'][0]['text']
  log('received',prompt=text)
  lifecycle(uuid,'queued')
  with WORK:
   QUEUE.append((uuid,text));WORK.notify()
";

/// Records approvals and answers them from a fixed decision.
struct Approver;

#[async_trait]
impl InteractionHandler for Approver {
    async fn approve(&self, request: ApprovalRequest) -> ApprovalDecision {
        assert_eq!(request.tool_name, "Bash");
        ApprovalDecision::Allow
    }

    async fn answer(&self, _request: QuestionRequest) -> Option<QuestionAnswer> {
        None
    }
}

struct Fixture {
    dir: tempfile::TempDir,
    log: std::path::PathBuf,
    client: InProcessRuntimeClient,
    handle: RuntimeHandle,
}

/// A started turn whose events are drained concurrently, as an application
/// does; an unread event stream would apply backpressure to the provider.
struct Running {
    turn_messages: temps_agent_runtime::retained::TurnMessageHandle,
    interrupt: temps_agent_runtime::retained::TurnInterruptHandle,
    events: JoinHandle<Vec<TurnEvent>>,
    completion: JoinHandle<Result<TurnResult, RuntimeFailure>>,
}

impl Running {
    fn new(turn: TurnHandle) -> Self {
        let turn_messages = turn.message_handle();
        let interrupt = turn.interrupt_handle();
        let (mut stream, completion) = turn.into_parts();
        Self {
            turn_messages,
            interrupt,
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
    async fn new(retained: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let executable = dir.path().join("claude-fixture");
        let log = dir.path().join("events.jsonl");
        fs::write(
            &executable,
            SCRIPT
                .replace("LOG_PATH", &serde_json::to_string(&log).unwrap())
                .replace("DIR_PATH", &serde_json::to_string(dir.path()).unwrap()),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
        let mut builder = AgentRuntime::builder();
        builder.register(Claude::with_executable(executable));
        if retained {
            builder = builder.provider_process_retention(ProviderProcessRetention {
                max_processes: 1,
                idle_timeout: Duration::from_secs(30),
                initialization_timeout: Duration::from_secs(10),
                active_inactivity_timeout: Some(Duration::from_secs(10)),
            });
        }
        let client = InProcessRuntimeClient::new(builder.build().unwrap());
        let handle = client
            .acquire(RuntimeSpec::new(
                RuntimeId::new("claude-live-messages").unwrap(),
                Provider::Claude,
                dir.path(),
            ))
            .await
            .unwrap();
        Self {
            dir,
            log,
            client,
            handle,
        }
    }

    fn input(id: &str, prompt: &str) -> TurnInput {
        TurnInput::new(InvocationId::new(id).unwrap(), prompt)
    }

    async fn start(&self, id: &str, prompt: &str) -> Running {
        Running::new(
            self.handle
                .start_turn(Self::input(id, prompt))
                .await
                .unwrap(),
        )
    }

    async fn start_with_approver(&self, id: &str, prompt: &str) -> Running {
        Running::new(
            self.handle
                .start_turn_with_interactions(Self::input(id, prompt), Arc::new(Approver))
                .await
                .unwrap(),
        )
    }

    fn release(&self, name: &str) {
        fs::write(self.dir.path().join(name), b"done").unwrap();
    }

    fn events(&self) -> Vec<serde_json::Value> {
        fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn logged(&self, kind: &str) -> usize {
        self.events().iter().filter(|v| v["kind"] == kind).count()
    }

    fn spawns(&self) -> usize {
        self.logged("spawn")
    }

    async fn wait_for_prompt(&self, prompt: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !self
                .events()
                .iter()
                .any(|v| v["kind"] == "prompt" && v["prompt"] == prompt)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("fixture never started {prompt}: {:?}", self.events()));
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

    async fn dispose(&self) {
        self.client
            .dispose(&RuntimeId::new("claude-live-messages").unwrap())
            .await
            .unwrap();
    }
}

fn task_activity(events: &[TurnEvent], task: &str, kind: AgentTaskActivityKind) -> bool {
    events.iter().any(|event| {
        matches!(
            event,
            TurnEvent::TaskActivity { activity }
                if activity.task_id == task && activity.kind == kind
        )
    })
}

#[tokio::test]
async fn a_message_sent_mid_turn_is_answered_by_the_same_turn() {
    let f = Fixture::new(true).await;
    assert!(f.handle.driver_capabilities().live_messages);
    let turn = f.start("one", "work").await;
    f.wait_for_log("work-start").await;

    turn.turn_messages
        .send("also check the tests")
        .await
        .expect("a running turn accepts messages");
    f.wait_for_log("received").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !turn.completion.is_finished(),
        "the turn stays open while its foreground work runs"
    );

    f.release("fg-release");
    let (_, result) = turn.finish().await;
    assert_eq!(
        result.unwrap().text,
        "built and reply:also check the tests",
        "the message is answered within the same turn"
    );
    assert_eq!(f.turn_count_received(), 2);
    assert_eq!(f.spawns(), 1);
    f.dispose().await;
}

#[tokio::test]
async fn a_finished_turn_rejects_further_messages() {
    let f = Fixture::new(true).await;
    let turn = f
        .handle
        .start_turn(Fixture::input("one", "hello"))
        .await
        .unwrap();
    let messages = turn.message_handle();
    assert_eq!(turn.wait().await.unwrap().text, "reply:hello");
    let error = messages
        .send("too late")
        .await
        .expect_err("a finished turn accepts no messages");
    assert_eq!(error.kind, RuntimeFailureKind::InvalidRequest);
    assert!(
        error.message.contains("start a new turn"),
        "{}",
        error.message
    );
    assert!(f.events().iter().all(|v| v["prompt"] != "too late"));
    f.dispose().await;
}

#[tokio::test]
async fn messages_need_a_retained_process() {
    let f = Fixture::new(false).await;
    assert!(!f.handle.driver_capabilities().live_messages);
    let turn = f.start("one", "work").await;
    f.wait_for_log("work-start").await;
    let error = turn
        .turn_messages
        .send("not deliverable")
        .await
        .expect_err("a one-shot process cannot accept messages");
    assert_eq!(error.kind, RuntimeFailureKind::CapabilityUnavailable);
    f.release("fg-release");
    assert_eq!(turn.finish().await.1.unwrap().text, "built");
    f.dispose().await;
}

#[tokio::test]
async fn interrupting_stops_foreground_work_and_keeps_background_work() {
    let f = Fixture::new(true).await;
    let turn = f.start("one", "spawn-bg-then-work").await;
    f.wait_for_log("work-start").await;
    f.wait_for_log("bg-start").await;

    assert_eq!(
        turn.interrupt.interrupt().await,
        InterruptOutcome::Interrupted
    );
    let (_, result) = turn.finish().await;
    assert_eq!(result.unwrap_err().kind, RuntimeFailureKind::Cancelled);
    assert_eq!(
        f.logged("work-stopped"),
        1,
        "the foreground command stopped"
    );
    assert_eq!(f.logged("bg-finished"), 0, "the subagent is still running");

    // Output the subagent produced while no turn ran reaches the next turn,
    // which also inherits its completion and the parent's answer to it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let next = f.start("two", "second").await;
    f.wait_for_prompt("second").await;
    f.release("bg-release");
    let (events, result) = next.finish().await;
    let text = result.unwrap().text;
    assert!(
        text.contains("reply:second") && text.contains("FOLLOWUP"),
        "{text}"
    );
    assert!(task_activity(
        &events,
        "bg-1",
        AgentTaskActivityKind::Progress
    ));
    assert!(task_activity(
        &events,
        "bg-1",
        AgentTaskActivityKind::Completed
    ));
    assert_eq!(f.logged("bg-finished"), 1);
    assert_eq!(f.spawns(), 1, "{:?}", f.events());
    f.dispose().await;
}

#[tokio::test]
async fn background_work_finishing_between_turns_reaches_the_next_turn() {
    let f = Fixture::new(true).await;
    let turn = f.start("one", "spawn-bg-then-work").await;
    f.wait_for_log("work-start").await;
    assert_eq!(
        turn.interrupt.interrupt().await,
        InterruptOutcome::Interrupted
    );
    let (_, interrupted) = turn.finish().await;
    assert_eq!(
        interrupted.expect_err("interrupted").kind,
        RuntimeFailureKind::Cancelled
    );

    // The subagent finishes and Claude answers its notification while no
    // turn runs. The parked process keeps that output for the next turn,
    // which must still wait for its own answer rather than end on it.
    f.release("bg-release");
    f.wait_for_log("bg-finished").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let next = f.start("two", "second").await;
    let (events, result) = next.finish().await;
    assert_eq!(result.unwrap().text, "reply:second");
    assert!(task_activity(
        &events,
        "bg-1",
        AgentTaskActivityKind::Completed
    ));
    assert!(events
        .iter()
        .any(|event| matches!(event, TurnEvent::TextDelta { text } if text == "FOLLOWUP")));
    assert_eq!(f.spawns(), 1, "{:?}", f.events());
    f.dispose().await;
}

#[tokio::test]
async fn interrupting_without_background_work_keeps_the_process() {
    let f = Fixture::new(true).await;
    let turn = f.start("one", "work").await;
    f.wait_for_log("work-start").await;
    assert_eq!(
        turn.interrupt.interrupt().await,
        InterruptOutcome::Interrupted
    );
    assert_eq!(f.logged("work-stopped"), 1);

    let next = f.start("two", "second").await;
    assert_eq!(next.finish().await.1.unwrap().text, "reply:second");
    assert_eq!(f.spawns(), 1, "{:?}", f.events());
    f.dispose().await;
}

#[tokio::test]
async fn interrupting_also_stops_messages_queued_behind_the_work() {
    let f = Fixture::new(true).await;
    let turn = f.start("one", "work").await;
    f.wait_for_log("work-start").await;
    turn.turn_messages.send("queued").await.unwrap();
    f.wait_for_log("received").await;

    assert_eq!(
        turn.interrupt.interrupt().await,
        InterruptOutcome::Interrupted
    );
    assert_eq!(
        f.events()
            .iter()
            .filter(|v| v["kind"] == "cancelled" && v["prompt"] == "queued")
            .count(),
        1,
        "{:?}",
        f.events()
    );

    let next = f.start("two", "second").await;
    assert_eq!(next.finish().await.1.unwrap().text, "reply:second");
    assert_eq!(f.spawns(), 1);
    f.dispose().await;
}

#[tokio::test]
async fn a_background_approval_waits_for_the_next_turn() {
    let f = Fixture::new(true).await;
    let turn = f.start("one", "spawn-bg-approval").await;
    f.wait_for_log("bg-start").await;
    // The user stops the turn; its subagent keeps running and later asks for
    // approval while no turn is running. The request is held for the next
    // turn instead of being refused.
    assert_eq!(
        turn.interrupt.interrupt().await,
        InterruptOutcome::Interrupted
    );
    f.release("approval-trigger");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(f.logged("approval"), 0, "{:?}", f.events());

    let next = f.start_with_approver("two", "second").await;
    f.wait_for_log("approval").await;
    assert!(f
        .events()
        .iter()
        .any(|v| v["kind"] == "approval" && v["behavior"] == "allow"));
    f.release("bg-release");
    let (_, result) = next.finish().await;
    assert!(result.unwrap().text.contains("reply:second"));
    assert_eq!(f.spawns(), 1);
    f.dispose().await;
}

impl Fixture {
    fn turn_count_received(&self) -> usize {
        self.logged("received")
    }
}
