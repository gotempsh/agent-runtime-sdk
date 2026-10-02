//! Verify with a real Claude CLI that a `Workflow` run is reported as a
//! structured workflow task and that each agent's activity can be read back.
//!
//! The turn runs a two-phase workflow whose agents use tools. The smoke
//! passes once the task snapshot names both phases and all agents as
//! completed, activity records each agent's state changes instead of every
//! tick, and every agent's transcript reads back with its tool calls.
//!
//! Usage: `cargo run --example claude_workflow_smoke -- [model]`

use std::sync::Mutex;

use async_trait::async_trait;
use temps_agent_runtime::providers::Claude;
use temps_agent_runtime::{
    AgentRuntime, AgentTask, AgentTaskActivity, AgentWorkflowAgentState, EventSink, PermissionMode,
    Provider, TurnEvent, TurnRequest,
};

#[derive(Default)]
struct Recorder {
    tasks: Mutex<Vec<AgentTask>>,
    activity: Mutex<Vec<AgentTaskActivity>>,
}

#[async_trait]
impl EventSink for Recorder {
    async fn emit(&self, event: TurnEvent) -> temps_agent_runtime::Result<()> {
        match event {
            TurnEvent::TasksChanged { tasks } => {
                *self
                    .tasks
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = tasks;
            }
            TurnEvent::TaskActivity { activity } => self
                .activity
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(activity),
            _ => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let model = std::env::args().nth(1).unwrap_or_else(|| "haiku".into());
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("notes.txt"), "alpha\nbeta\n")?;
    let script = "export const meta = { name: 'smoke', description: 'Workflow smoke', \
        phases: [{ title: 'Scan' }, { title: 'Report' }] }\n\
        const [a, b] = await parallel([\n\
          () => agent('Use the Read tool to read notes.txt in the current directory, then reply with its first word.', { label: 'scan:read', phase: 'Scan' }),\n\
          () => agent('Use the Bash tool to run: ls, then reply with the number of entries.', { label: 'scan:list', phase: 'Scan' }),\n\
        ])\n\
        const c = await agent('Reply with exactly REPORTED.', { label: 'report', phase: 'Report' })\n\
        return { a, b, c }";
    let mut request = TurnRequest::new(
        Provider::Claude,
        directory.path(),
        format!(
            "Use a workflow: call the Workflow tool with exactly this script, wait for it to \
             finish, then reply DONE.\n\n{script}"
        ),
    );
    request.model = Some(model);
    request.permission_mode = PermissionMode::FullAccess;
    request.timeout = std::time::Duration::from_secs(400);

    let mut builder = AgentRuntime::builder();
    builder.register(Claude::default());
    let runtime = builder.build()?;
    let recorder = Recorder::default();
    let result = runtime.run(request, &recorder, None).await?;
    println!("turn: {:?} {:?}", result.status, result.text);

    let tasks = recorder
        .tasks
        .lock()
        .map(|tasks| tasks.clone())
        .unwrap_or_default();
    let task = tasks
        .iter()
        .find(|task| task.kind == "workflow")
        .ok_or("no workflow task was reported")?;
    let workflow = task
        .workflow
        .as_ref()
        .ok_or("the workflow task has no structure")?;
    println!(
        "workflow {:?} run {:?}, transcripts in {:?}",
        workflow.name, workflow.run_id, workflow.transcript_dir
    );
    let phases: Vec<_> = workflow
        .phases
        .iter()
        .map(|phase| phase.title.as_str())
        .collect();
    println!("phases: {phases:?}");
    let mut ok = phases == ["Scan", "Report"] && workflow.agents.len() == 3;

    let claude = Claude::default();
    for agent in &workflow.agents {
        println!(
            "agent {} [{}] {:?}: {:?} tokens, {:?} tools, last tool {:?} {:?}, result {:?}",
            agent.label,
            agent.phase_title.as_deref().unwrap_or("-"),
            agent.state,
            agent.tokens,
            agent.tool_calls,
            agent.last_tool_name,
            agent.last_tool_summary,
            agent.result_preview
        );
        ok &= agent.state == AgentWorkflowAgentState::Completed;
        let Some(path) = workflow.agent_transcript_path(agent) else {
            println!("  no transcript path");
            ok = false;
            continue;
        };
        let transcript = std::fs::read_to_string(&path)?;
        let entries = claude.transcript_activity(&transcript, 200);
        let tools: Vec<_> = entries
            .iter()
            .filter_map(|entry| match &entry.event {
                TurnEvent::ToolCall { name, status, .. } => Some(format!("{name} {status:?}")),
                _ => None,
            })
            .collect();
        println!(
            "  transcript: {} entries, tool calls {tools:?}",
            entries.len()
        );
        ok &= !entries.is_empty();
        if agent.label.starts_with("scan:") {
            ok &= tools.iter().any(|tool| tool.ends_with("Succeeded"));
        }
    }

    let activity = recorder
        .activity
        .lock()
        .map(|activity| activity.clone())
        .unwrap_or_default();
    let changes: Vec<_> = activity
        .iter()
        .filter(|activity| activity.task_id == task.id)
        .filter_map(|activity| {
            activity
                .summary
                .clone()
                .filter(|_| activity.workflow_agent.is_some())
        })
        .collect();
    println!("agent state changes: {changes:?}");
    ok &= changes.iter().any(|change| change == "report completed");

    if ok {
        println!("PASS");
        Ok(())
    } else {
        Err("the workflow was not reported as expected".into())
    }
}
