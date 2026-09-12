import { expect, test, type Page } from "@playwright/test";

function collectBrowserFailures(page: Page) {
  const failures: string[] = [];
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  page.on("requestfailed", (request) => {
    const intentionalEventSourceClose = request.resourceType() === "eventsource" && request.failure()?.errorText === "net::ERR_ABORTED";
    if (!intentionalEventSourceClose) {
      failures.push(`requestfailed: ${request.method()} ${request.url()} ${request.failure()?.errorText}`);
    }
  });
  page.on("response", (response) => {
    if (response.status() >= 500) failures.push(`response: ${response.status()} ${response.url()}`);
  });
  return failures;
}

async function openFixtureChat(page: Page, prompt: string, scenario: "approval" | "failure" = "approval") {
  const response = await page.request.post("/api/chats", {
    data: {
      prompt,
      mode: "demo",
      provider: "claude",
      permission: "plan",
      scenario,
      model: null,
      reasoning: null,
      harness_options: {},
      target: { kind: "local" },
    },
  });
  expect(response.ok()).toBe(true);
  const view = await response.json() as { chat: { id: string } };
  await page.goto(`/?chat=${encodeURIComponent(view.chat.id)}`);
  return view.chat.id;
}

async function openConnectionPicker(page: Page) {
  await page.getByLabel("Execution target", { exact: true }).selectOption("add");
  await expect(page.getByRole("heading", { name: "Add an execution target." })).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
}

function readyClaudeInventory() {
  return {
    transport: "local",
    transport_capabilities: {
      remote: false,
      interactive_stdin: true,
      reconnect: false,
      managed_processes: true,
      process_tree_termination: true,
      sandbox: {},
    },
    harnesses: [{
      provider: "claude",
      status: "ready",
      readiness: { installed: true, executable: "claude", version: "5.0.0", detail: "Ready" },
      permissions: { default: true, accept_edits: true, plan: true, full_access: true, custom: true, live_approvals: true, live_questions: true },
      control_groups: [{
        id: "permission_mode",
        label: "Permission",
        kind: "permission",
        options: [
          { id: "default", label: "Ask first", description: "Ask before tools", is_default: true, dangerous: false },
          { id: "plan", label: "Plan", description: "Plan only", is_default: false, dangerous: false },
        ],
      }],
      models: {
        status: "ready",
        source: "fixture",
        models: [{
          id: "claude-opus-5",
          label: "Opus 5",
          description: "Opus 5",
          is_default: true,
          reasoning_efforts: [
            { id: "high", label: "High", description: "High thinking", is_default: true },
            { id: "max", label: "Max", description: "Maximum thinking", is_default: false },
          ],
          service_tiers: [],
        }],
        error: null,
      },
      account_usage: {
        provider: "claude",
        plan: "pro",
        windows: [
          { id: "five_hour", kind: "session", used_percent: 63, duration_minutes: 300, resets_at_unix_seconds: Math.floor(Date.now() / 1000) + 7200 },
          { id: "seven_day", kind: "weekly", used_percent: 41, duration_minutes: 10080, resets_at_unix_seconds: Math.floor(Date.now() / 1000) + 432000 },
        ],
        credits: null,
      },
      limitations: [],
      error: null,
    }],
  };
}

test("renders the console immediately and preloads skills for prompt autocomplete", async ({ page }) => {
  const failures = collectBrowserFailures(page);
  let extensionRequests = 0;
  await page.route("**/api/ssh-connections", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/chats?*", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/discovery", (route) => route.fulfill({ json: readyClaudeInventory() }));
  await page.route("**/api/working-directories", async (route) => {
    const request = route.request().postDataJSON() as { input?: string };
    await route.fulfill({
      json: {
        home: "/work",
        directories: ["/work", "/work/project"],
        exact_match: request.input === "/work" || request.input === "/work/project",
      },
    });
  });
  await page.route("**/api/extensions", async (route) => {
    extensionRequests += 1;
    const request = route.request().postDataJSON() as { provider?: string; working_directory?: string; skill_query?: string | null };
    expect(request).toMatchObject({ provider: "claude", working_directory: "/work", skill_query: null });
    await route.fulfill({
      json: {
        provider: "claude",
        transport: "local",
        working_directory: "/work",
        skills: [{
          id: "review-pr",
          description: "Review changes in the current project",
          path: "/work/.agents/skills/review-pr/SKILL.md",
          scope: "project",
          source: "agents",
        }],
        mcp_servers: [],
        manage_user_skills: { allowed: true, denial_kind: null, reason: null },
        manage_project_skills: { allowed: true, denial_kind: null, reason: null },
        manage_user_mcp_servers: { allowed: false, denial_kind: "provider_unsupported", reason: "Not supported by this harness." },
        manage_project_mcp_servers: { allowed: false, denial_kind: "provider_unsupported", reason: "Not supported by this harness." },
        warnings: [],
      },
    });
  });

  await page.goto("/");
  await expect(page.getByRole("complementary", { name: "Chats" })).toBeVisible();
  await expect(page.getByRole("complementary", { name: "Activity" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Choose a folder and start chatting." })).toBeVisible();
  await expect(page.getByText("Choose where the harness lives.", { exact: true })).toHaveCount(0);
  await expect(page.getByText("Skills & MCP", { exact: true })).toHaveCount(0);
  await expect(page.getByLabel("Execution target", { exact: true })).toHaveValue("local");
  await expect(page.getByText("Target for this chat", { exact: true })).toHaveCount(0);
  const folder = page.getByRole("combobox", { name: "Folder for this chat" });
  await expect(folder).toHaveValue("");
  await expect(page.getByRole("button", { name: "Send" })).toBeDisabled();
  await folder.focus();
  await page.getByRole("option", { name: "/work", exact: true }).click();
  await expect(folder).toHaveValue("/work");
  await expect(page.getByLabel("Harness").locator("option")).toHaveText("Claude Code");
  await expect(page.getByLabel("Model")).toHaveValue("claude-opus-5");
  await expect(page.getByLabel("Thinking")).toHaveValue("high");
  await expect(page.getByLabel("Permission")).toHaveValue("default");
  await page.getByRole("button", { name: "Show context and account usage" }).click();
  await expect(page.getByRole("region", { name: "Context and account usage" })).toContainText("Session");
  await expect(page.getByRole("region", { name: "Context and account usage" })).toContainText("63%");
  await expect(page.getByRole("region", { name: "Context and account usage" })).toContainText("Weekly");
  await page.getByLabel("Model").selectOption("claude-opus-5");
  await page.getByLabel("Thinking").selectOption("max");
  await page.getByLabel("Permission").selectOption("plan");
  await expect.poll(() => extensionRequests).toBe(1);

  const prompt = page.getByLabel("Prompt");
  await prompt.fill("/rev");
  await expect(page.getByRole("option", { name: /review-pr/ })).toBeVisible();
  expect(extensionRequests).toBe(1);
  await prompt.press("Enter");
  await expect(prompt).toHaveValue("/review-pr ");
  expect(failures).toEqual([]);
});

test("switches directly to a configured host from the header", async ({ page }) => {
  const connection = {
    id: "ssh_mac_studio",
    label: "Mac Studio",
    host: "192.168.1.9",
    user: "joseviejo",
    port: 22,
    authentication: "agent",
    identity_file: null,
    known_hosts_file: null,
    accept_new_host_key: false,
    has_password: false,
    created_at_ms: Date.now(),
    updated_at_ms: Date.now(),
  };
  const discoveryTargets: Array<Record<string, unknown>> = [];
  await page.route("**/api/ssh-connections", (route) => route.fulfill({ json: [connection] }));
  await page.route("**/api/chats?*", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/discovery", async (route) => {
    discoveryTargets.push((route.request().postDataJSON() as { target: Record<string, unknown> }).target);
    await route.fulfill({ json: readyClaudeInventory() });
  });
  await page.route("**/api/working-directories", (route) => route.fulfill({
    json: { home: "/Users/example", directories: ["/Users/example"], exact_match: false },
  }));

  await page.goto("/");
  const target = page.getByLabel("Execution target", { exact: true });
  await target.selectOption(`ssh:${connection.id}`);

  await expect(target).toHaveValue(`ssh:${connection.id}`);
  await expect(page.getByRole("heading", { name: "Choose a folder and start chatting." })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Add an execution target." })).toHaveCount(0);
  await expect(page.getByText("Target for this chat", { exact: true })).toHaveCount(0);
  await expect(page.getByText(/Commands will run on Mac Studio/)).toBeVisible();
  await expect.poll(() => discoveryTargets.some((item) => item.kind === "ssh" && item.connection_id === connection.id)).toBe(true);
});

test("shows an actionable empty state when a target has no ready harnesses", async ({ page }) => {
  await page.route("**/api/ssh-connections", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/chats?*", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/discovery", (route) => route.fulfill({
    json: {
      transport: "local",
      transport_capabilities: {},
      harnesses: [],
    },
  }));
  await page.route("**/api/working-directories", (route) => route.fulfill({
    json: { home: "/work", directories: ["/work"], exact_match: false },
  }));

  await page.goto("/");
  await expect(page.getByText("No harnesses available on this target", { exact: true })).toBeVisible();
  await expect(page.getByLabel("Prompt")).toHaveCount(0);
  await page.getByRole("button", { name: "Add target" }).click();
  await expect(page.getByRole("heading", { name: "Add an execution target." })).toBeVisible();
});

test("renders and resolves an SDK-normalized agent question", async ({ page }) => {
  const now = Date.now();
  const chat = {
    id: "chat_question",
    title: "Choose a fruit",
    mode: "installed",
    provider: "claude",
    permission: "default",
    scenario: "approval",
    target: "local",
    connection_kind: "local",
    connection_id: null,
    connection_key: "local",
    working_directory: ".",
    status: "input_needed",
    draft: "",
    session_id: "session-question",
    model: "claude-opus-5",
    reasoning: "high",
    harness_options: { permission_mode: "default" },
    error: null,
    created_at_ms: now,
    updated_at_ms: now,
  };
  const questionEvent = {
    sequence: 1,
    timestamp_ms: now,
    kind: "turn_event",
    payload: {
      type: "question_requested",
      id: "question-fruit",
      prompts: [{
        header: "Fruit",
        question: "Which fruit do you prefer?",
        multiSelect: false,
        options: [
          { label: "Plantain", description: "Choose plantain." },
          { label: "Banana", description: "Choose banana." },
        ],
      }],
      questions: [],
    },
  };
  let submitted: unknown = null;

  await page.route("**/api/discovery", (route) => route.fulfill({ json: readyClaudeInventory() }));
  await page.route("**/api/extensions", (route) => route.fulfill({ json: {
    provider: "claude", transport: "local", working_directory: ".", skills: [], mcp_servers: [],
    manage_user_skills: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_project_skills: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_user_mcp_servers: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_project_mcp_servers: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    warnings: [],
  } }));
  await page.route("**/api/ssh-connections", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/chats/chat_question/questions/question-fruit", async (route) => {
    submitted = route.request().postDataJSON();
    await route.fulfill({ status: 202, body: "" });
  });
  await page.route("**/api/chats/chat_question/events*", (route) => route.fulfill({
    status: 200,
    contentType: "text/event-stream",
    body: "",
  }));
  await page.route("**/api/chats/chat_question/queue", (route) => route.fulfill({ json: [] }));
  await page.route(/\/api\/chats\/chat_question$/, (route) => route.fulfill({ json: { chat, messages: [], events: [questionEvent] } }));
  await page.route("**/api/chats?*", (route) => route.fulfill({ json: [chat] }));

  await page.goto("/?chat=chat_question");
  await expect(page.getByTestId("question-card")).toContainText("Which fruit do you prefer?");
  await page.getByRole("button", { name: /Banana/ }).click();
  await page.getByRole("button", { name: "Send answer" }).click();
  await expect.poll(() => submitted).toEqual({ answers: { "Which fruit do you prefer?": "Banana" } });
});

test("renders a Claude plan proposal as an explicit accept or reject boundary", async ({ page }) => {
  const now = Date.now();
  const chat = {
    id: "chat_plan_approval",
    title: "Plan the migration",
    mode: "installed",
    provider: "claude",
    permission: "plan",
    scenario: "approval",
    target: "local",
    connection_kind: "local",
    connection_id: null,
    connection_key: "local",
    working_directory: ".",
    status: "approval_needed",
    draft: "",
    session_id: "session-plan",
    model: "claude-opus-5",
    reasoning: "high",
    harness_options: { permission_mode: "plan" },
    error: null,
    created_at_ms: now,
    updated_at_ms: now,
  };
  const approvalEvent = {
    sequence: 1,
    timestamp_ms: now,
    kind: "turn_event",
    payload: {
      type: "plan_approval_requested",
      id: "approve-plan",
      tool_name: "ExitPlanMode",
      description: "Apply the proposed migration plan.",
      input: { plan: "Update the adapter, then verify the stream." },
    },
  };
  let submitted: unknown = null;

  await page.route("**/api/discovery", (route) => route.fulfill({ json: readyClaudeInventory() }));
  await page.route("**/api/extensions", (route) => route.fulfill({ json: {
    provider: "claude", transport: "local", working_directory: ".", skills: [], mcp_servers: [],
    manage_user_skills: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_project_skills: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_user_mcp_servers: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_project_mcp_servers: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    warnings: [],
  } }));
  await page.route("**/api/ssh-connections", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/chats/chat_plan_approval/approvals/approve-plan", async (route) => {
    submitted = route.request().postDataJSON();
    await route.fulfill({ status: 202, body: "" });
  });
  await page.route("**/api/chats/chat_plan_approval/events*", (route) => route.fulfill({ status: 200, contentType: "text/event-stream", body: "" }));
  await page.route("**/api/chats/chat_plan_approval/queue", (route) => route.fulfill({ json: [] }));
  await page.route(/\/api\/chats\/chat_plan_approval$/, (route) => route.fulfill({ json: { chat, messages: [], events: [approvalEvent] } }));
  await page.route("**/api/chats?*", (route) => route.fulfill({ json: [chat] }));

  await page.goto("/?chat=chat_plan_approval");
  const approval = page.getByTestId("approval-card");
  await expect(approval).toContainText("Plan approval required");
  await expect(approval).toContainText("Claude proposed a plan");
  await expect(page.getByLabel("Permission")).toHaveValue("plan");
  await page.getByRole("button", { name: "Accept plan" }).click();
  await expect.poll(() => submitted).toEqual({ decision: "allow" });
});

test("runs /compact as an SDK action without adding a chat message", async ({ page }) => {
  const now = Date.now();
  const chat = {
    id: "chat_compact",
    title: "Long Claude session",
    mode: "installed",
    provider: "claude",
    permission: "default",
    scenario: "approval",
    target: "local",
    connection_kind: "local",
    connection_id: null,
    connection_key: "local",
    working_directory: ".",
    status: "succeeded",
    draft: "",
    session_id: "session-compact",
    model: "claude-opus-5",
    reasoning: "high",
    harness_options: { permission_mode: "default" },
    error: null,
    created_at_ms: now,
    updated_at_ms: now,
  };
  const originalSession = {
    sequence: 1,
    timestamp_ms: now + 1,
    kind: "turn_event",
    payload: { type: "session_started", session_id: "session-compact" },
  };
  const requested = {
    sequence: 2,
    timestamp_ms: now + 2,
    kind: "compaction_requested",
    payload: { invocation_id: "compact-chat_compact-1" },
  };
  const started = {
    sequence: 3,
    timestamp_ms: now + 3,
    kind: "turn_event",
    payload: { type: "compaction_started", trigger: "manual" },
  };
  const repeatedSession = {
    sequence: 4,
    timestamp_ms: now + 4,
    kind: "turn_event",
    payload: { type: "session_started", session_id: "session-compact" },
  };
  let compactBody: unknown = null;
  let messageRequests = 0;

  await page.route("**/api/discovery", (route) => route.fulfill({ json: readyClaudeInventory() }));
  await page.route("**/api/extensions", (route) => route.fulfill({ json: {
    provider: "claude", transport: "local", working_directory: ".", skills: [], mcp_servers: [],
    manage_user_skills: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_project_skills: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_user_mcp_servers: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_project_mcp_servers: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    warnings: [],
  } }));
  await page.route("**/api/ssh-connections", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/chats/chat_compact/events*", (route) => route.fulfill({ status: 200, contentType: "text/event-stream", body: "" }));
  await page.route("**/api/chats/chat_compact/queue", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/chats/chat_compact/messages", (route) => {
    messageRequests += 1;
    return route.fulfill({ status: 500, json: { error: "The command must not be sent as a message." } });
  });
  await page.route("**/api/chats/chat_compact/compact", async (route) => {
    compactBody = route.request().postDataJSON();
    await route.fulfill({
      status: 202,
      json: {
        chat: { ...chat, status: "running", updated_at_ms: now + 4 },
        messages: [],
        events: [originalSession, requested, started, repeatedSession],
      },
    });
  });
  await page.route(/\/api\/chats\/chat_compact$/, (route) => route.fulfill({ json: { chat, messages: [], events: [] } }));
  await page.route("**/api/chats?*", (route) => route.fulfill({ json: [chat] }));

  await page.goto("/?chat=chat_compact");
  await page.getByLabel("Prompt").fill("/comp");
  const action = page.getByRole("option", { name: /\/compact/ });
  await expect(action).toContainText("Summarize this Claude session");
  await action.click();

  await expect.poll(() => compactBody).toMatchObject({
    provider: "claude",
    target: { kind: "local" },
    working_directory: ".",
  });
  await expect(page.getByLabel("Prompt")).toHaveValue("");
  await expect(page.getByTestId("activity-list")).toContainText("compaction_requested");
  await expect(page.getByTestId("activity-list").getByText("session_resumed", { exact: true })).toBeVisible();
  await expect(page.getByTestId("activity-list")).toContainText("Reattached session session-compact for compaction");
  await expect(page.getByTestId("working-state")).toContainText("Compacting context");
  expect(messageRequests).toBe(0);
});

test("replays Claude native subagents with nested activity and tools", async ({ page }) => {
  const now = Date.now();
  const chat = {
    id: "chat_claude_subagent",
    title: "Inspect authentication",
    mode: "installed",
    provider: "claude",
    permission: "default",
    scenario: "approval",
    target: "local",
    connection_kind: "local",
    connection_id: null,
    connection_key: "local",
    working_directory: ".",
    status: "succeeded",
    draft: "",
    session_id: "session-subagent",
    model: "claude-opus-5",
    reasoning: "high",
    harness_options: { permission_mode: "default" },
    error: null,
    created_at_ms: now,
    updated_at_ms: now,
  };
  const messages = [
    { sequence: 1, role: "user", content: "Inspect the authentication flow", attachments: [], created_at_ms: now },
    { sequence: 2, role: "assistant", content: "The authentication flow is bounded and verified.", attachments: [], created_at_ms: now + 8 },
  ];
  const task = {
    id: "agent-native-1",
    kind: "subagent",
    description: "Inspect the authentication flow",
    status: "running",
    agent_type: "Explore",
    error: null,
    summary: "Reading the session boundary",
  };
  const events = [
    { sequence: 1, timestamp_ms: now, kind: "message_started", payload: { message_sequence: 1 } },
    { sequence: 2, timestamp_ms: now + 1, kind: "turn_event", payload: { type: "tasks_changed", tasks: [task] } },
    { sequence: 3, timestamp_ms: now + 2, kind: "turn_event", payload: { type: "task_activity", activity: { task_id: task.id, kind: "started", description: task.description, status: "running", agent_type: "Explore", summary: null, last_tool_name: null, spawn_depth: 1, usage: null } } },
    { sequence: 4, timestamp_ms: now + 3, kind: "turn_event", payload: { type: "tool_call", id: "tool-read-auth", name: "Read", status: "started", input: { file_path: "src/auth.rs" }, output: null, error: null, task_id: task.id } },
    { sequence: 5, timestamp_ms: now + 4, kind: "turn_event", payload: { type: "task_activity", activity: { task_id: task.id, kind: "progress", description: task.description, status: "running", agent_type: "Explore", summary: "Found the session boundary", last_tool_name: "Read", spawn_depth: 1, usage: { total_tokens: 640, tool_uses: 1, duration_ms: 1_240 } } } },
    { sequence: 6, timestamp_ms: now + 5, kind: "turn_event", payload: { type: "tool_call", id: "tool-read-auth", name: "Read", status: "succeeded", input: { file_path: "src/auth.rs" }, output: "Session validation is enforced before dispatch.", error: null, task_id: task.id } },
    { sequence: 7, timestamp_ms: now + 6, kind: "turn_event", payload: { type: "tasks_changed", tasks: [{ ...task, status: "completed", summary: "Authentication flow verified" }] } },
    { sequence: 8, timestamp_ms: now + 7, kind: "turn_event", payload: { type: "task_activity", activity: { task_id: task.id, kind: "completed", description: task.description, status: "completed", agent_type: "Explore", summary: "Authentication flow verified", last_tool_name: "Read", spawn_depth: 1, usage: { total_tokens: 710, tool_uses: 1, duration_ms: 1_480 } } } },
  ];

  await page.route("**/api/discovery", (route) => route.fulfill({ json: readyClaudeInventory() }));
  await page.route("**/api/extensions", (route) => route.fulfill({ json: {
    provider: "claude", transport: "local", working_directory: ".", skills: [], mcp_servers: [],
    manage_user_skills: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_project_skills: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_user_mcp_servers: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_project_mcp_servers: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    warnings: [],
  } }));
  await page.route("**/api/ssh-connections", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/chats/chat_claude_subagent/events*", (route) => route.fulfill({ status: 200, contentType: "text/event-stream", body: "" }));
  await page.route("**/api/chats/chat_claude_subagent/queue", (route) => route.fulfill({ json: [] }));
  await page.route(/\/api\/chats\/chat_claude_subagent$/, (route) => route.fulfill({ json: { chat, messages, events } }));
  await page.route("**/api/chats?*", (route) => route.fulfill({ json: [chat] }));

  await page.goto("/?chat=chat_claude_subagent");

  const disclosure = page.getByRole("button", { name: /1 Claude subagent/ });
  await expect(disclosure).toBeVisible();
  await expect(disclosure).toHaveAttribute("aria-expanded", "false");
  await disclosure.click();

  const subagent = page.getByTestId("claude-subagent");
  await expect(subagent).toContainText("Inspect the authentication flow");
  await expect(subagent).toContainText("Explore");
  await expect(subagent).toContainText("Completed");
  await subagent.getByRole("button", { name: /Inspect the authentication flow/ }).click();
  await expect(subagent).toContainText("Authentication flow verified");
  await expect(subagent).toContainText("710");
  await expect(subagent.getByTestId("tool-call")).toContainText("src/auth.rs");
  await expect(subagent.getByTestId("tool-call")).toContainText("Session validation is enforced before dispatch.");
  await expect(page.locator('[data-subagent-activity="true"]')).toHaveCount(7);

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(subagent).toBeVisible();
  await expect(page.getByTestId("claude-subagents")).toBeVisible();
  expect(await page.getByTestId("claude-subagents").evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
});

test("discovers local harnesses and exposes typed target errors", async ({ page }) => {
  const failures = collectBrowserFailures(page);
  const discoveryResponse = await page.request.post("/api/discovery", { data: { target: { kind: "local" } } });
  expect(discoveryResponse.ok()).toBe(true);
  const discovery = await discoveryResponse.json() as { harnesses: { provider: string; status: string }[] };
  expect(discovery.harnesses.map((harness) => harness.provider)).toEqual(["claude", "codex", "open_code"]);
  expect(discovery.harnesses.every((harness) => ["ready", "not_installed", "incompatible", "unavailable"].includes(harness.status))).toBe(true);
  await page.goto("/");
  await expect(page.getByLabel("Execution target", { exact: true })).toHaveValue("local");
  if (discovery.harnesses.some((harness) => harness.status === "ready")) {
    await expect(page.getByLabel("Harness")).toBeVisible();
  } else {
    await expect(page.getByText("No harnesses available on this target")).toBeVisible();
  }
  await expect(page.getByText("Use deterministic provider", { exact: true })).toHaveCount(0);

  await openConnectionPicker(page);
  await page.getByRole("textbox", { name: "Host", exact: true }).fill("invalid host");
  await page.getByRole("button", { name: "Check again" }).click();
  await expect(page.getByRole("alert")).toContainText("invalid configuration");
  expect(failures).toEqual([]);
});

test("submits a password-manager-autofilled SSH password", async ({ page }) => {
  const failures = collectBrowserFailures(page);
  let submittedPassword: string | null = null;
  await page.emulateMedia({ colorScheme: "dark" });
  await page.route("**/api/discovery", async (route) => {
    const body = route.request().postDataJSON() as {
      target?: { kind?: string; authentication?: { password?: string } };
    };
    if (body.target?.kind === "ssh") {
      submittedPassword = body.target.authentication?.password ?? null;
    }
    await route.fulfill({
      json: { transport: body.target?.kind ?? "local", transport_capabilities: {}, harnesses: [] },
    });
  });

  await page.goto("/");
  await openConnectionPicker(page);
  await page.getByRole("tab", { name: "SSH" }).click();
  await page.getByRole("textbox", { name: "Host", exact: true }).fill("worker.example.com");
  await page.getByRole("textbox", { name: "User", exact: true }).fill("agent");
  await page.locator('select[name="ssh_authentication"]').selectOption("password");

  const password = page.getByLabel("Password", { exact: true });
  await password.evaluate((element) => {
    const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
    setter?.call(element, "autofilled-secret");
  });
  await expect(password).toHaveValue("autofilled-secret");

  const check = page.getByRole("button", { name: "Check again" });
  await expect(check).toBeEnabled();
  await expect.poll(() => check.evaluate((element) => getComputedStyle(element).color)).toMatch(/(?:244|0\.967)/);
  await check.click();

  await expect.poll(() => submittedPassword).toBe("autofilled-secret");
  await expect(page.getByTestId("harness-inventory")).toContainText("ssh harnesses");
  expect(failures).toEqual([]);
});

test("saves SSH connections without returning their password", async ({ page }) => {
  const response = await page.request.post("/api/ssh-connections", {
    data: {
      label: "Playwright SSH",
      target: {
        kind: "ssh",
        connection_id: null,
        host: "worker.example.com",
        user: "agent",
        port: 22,
        authentication: { kind: "password", password: "test-only-secret" },
        known_hosts_file: null,
        accept_new_host_key: false,
      },
    },
  });
  expect(response.ok()).toBe(true);
  const saved = await response.json() as { id: string; has_password: boolean; password?: string };
  expect(saved.has_password).toBe(true);
  expect(saved.password).toBeUndefined();

  const list = await page.request.get("/api/ssh-connections");
  expect(list.ok()).toBe(true);
  const connections = await list.json() as Array<{ id: string; label: string; password?: string }>;
  expect(connections.find((connection) => connection.id === saved.id)?.label).toBe("Playwright SSH");
  expect(connections.find((connection) => connection.id === saved.id)?.password).toBeUndefined();

  const removed = await page.request.delete(`/api/ssh-connections/${encodeURIComponent(saved.id)}`);
  expect(removed.ok()).toBe(true);
});

test("rejects changing a chat from Local to another connection", async ({ page }) => {
  const created = await page.request.post("/api/chats", {
    data: {
      prompt: "Finish this fixture without an approval.",
      mode: "demo",
      provider: "claude",
      permission: "plan",
      scenario: "failure",
      model: null,
      reasoning: null,
      harness_options: {},
      target: { kind: "local" },
    },
  });
  expect(created.ok()).toBe(true);
  const view = await created.json() as { chat: { id: string; connection_kind: string; connection_key: string } };
  expect(view.chat.connection_kind).toBe("local");
  expect(view.chat.connection_key).toBe("local");

  await expect.poll(async () => {
    const response = await page.request.get(`/api/chats/${encodeURIComponent(view.chat.id)}`);
    return (await response.json() as { chat: { status: string } }).chat.status;
  }).toBe("failed");

  const changed = await page.request.post(`/api/chats/${encodeURIComponent(view.chat.id)}/messages`, {
    data: {
      prompt: "Try another target.",
      mode: "demo",
      provider: "claude",
      permission: "plan",
      scenario: "failure",
      model: null,
      reasoning: null,
      harness_options: {},
      target: {
        kind: "ssh",
        connection_id: null,
        host: "worker.example.com",
        user: "agent",
        port: 22,
        authentication: { kind: "agent" },
        known_hosts_file: null,
        accept_new_host_key: false,
      },
    },
  });
  expect(changed.status()).toBe(409);
  await expect(changed.json()).resolves.toMatchObject({ error: expect.stringContaining("cannot change execution connection") });
});

test("selects and keeps a remote working directory for an SSH chat", async ({ page }) => {
  const failures = collectBrowserFailures(page);
  let directoryRequests = 0;
  await page.route("**/api/ssh-connections", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/chats?*", (route) => route.fulfill({ json: [] }));
  await page.route("**/api/extensions", (route) => route.fulfill({ json: {
    provider: "claude", transport: "ssh", working_directory: "/Users/agent/projects/runtime",
    skills: [], mcp_servers: [],
    manage_user_skills: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_project_skills: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_user_mcp_servers: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    manage_project_mcp_servers: { allowed: false, denial_kind: "provider_unsupported", reason: "Unavailable" },
    warnings: [],
  } }));
  await page.route("**/api/working-directories", async (route) => {
    directoryRequests += 1;
    const body = route.request().postDataJSON() as { input?: string };
    const exact = body.input === "/Users/agent" || body.input === "/Users/agent/projects/runtime";
    await route.fulfill({
      json: {
        home: "/Users/agent",
        directories: ["/Users/agent", "/Users/agent/projects", "/Users/agent/projects/runtime"],
        exact_match: exact,
      },
    });
  });
  await page.route("**/api/discovery", async (route) => {
    const body = route.request().postDataJSON() as { target?: { kind?: string } };
    await route.fulfill({
      json: {
        transport: body.target?.kind ?? "local",
        transport_capabilities: {
          remote: body.target?.kind === "ssh",
          interactive_stdin: true,
          reconnect: false,
          managed_processes: true,
          process_tree_termination: true,
          sandbox: {},
        },
        harnesses: body.target?.kind === "ssh" ? [{
          provider: "claude",
          status: "ready",
          readiness: { installed: true, executable: "/opt/homebrew/bin/claude", version: "test", detail: "ready" },
          permissions: {
            default: true,
            accept_edits: true,
            plan: true,
            full_access: true,
            custom: true,
            live_approvals: true,
            live_questions: true,
          },
          control_groups: [],
          models: { status: "unsupported", source: "test", models: [], error: null },
          limitations: [],
          error: null,
        }] : [],
      },
    });
  });

  await page.goto("/");
  await openConnectionPicker(page);
  await page.getByRole("textbox", { name: "Host", exact: true }).fill("worker.example.com");
  await page.getByRole("button", { name: "Check again" }).click();
  await expect(page.getByTestId("harness-inventory")).toContainText("Ready");
  expect(directoryRequests).toBe(0);
  await expect(page.getByText("Folder for this chat")).toHaveCount(0);

  await page.getByRole("button", { name: "Use Claude Code" }).click();

  const directory = page.getByRole("combobox", { name: "Folder for this chat" });
  await expect(directory).toHaveValue("");
  await expect.poll(() => directoryRequests).toBeGreaterThan(0);
  await directory.focus();
  const folderOptions = page.locator("#chat-directory-options");
  await expect(folderOptions.getByRole("option").first()).toHaveText("/Users/agent");
  await folderOptions.getByRole("option", { name: "/Users/agent", exact: true }).click();
  await expect(directory).toHaveValue("/Users/agent");
  await directory.fill("/Users/agent/proj");
  await folderOptions.getByRole("option", { name: "/Users/agent/projects/runtime" }).click();
  await expect(directory).toHaveValue("/Users/agent/projects/runtime");
  expect(failures).toEqual([]);
});

test("keeps onboarding vertically scrollable in a short viewport", async ({ page }) => {
  const failures = collectBrowserFailures(page);
  await page.route("**/api/discovery", (route) => route.fulfill({ json: readyClaudeInventory() }));
  await page.route("**/api/chats?*", (route) => route.fulfill({ json: [] }));
  await page.setViewportSize({ width: 390, height: 560 });
  await page.goto("/");
  await openConnectionPicker(page);

  const onboarding = page.locator(".onboarding-shell");
  await expect(onboarding).toBeVisible();

  const before = await onboarding.evaluate((element) => ({
    clientHeight: element.clientHeight,
    overflowY: getComputedStyle(element).overflowY,
    scrollHeight: element.scrollHeight,
  }));
  expect(before.overflowY).toBe("auto");
  expect(before.scrollHeight).toBeGreaterThan(before.clientHeight);

  await onboarding.evaluate((element) => element.scrollTo({ top: element.scrollHeight }));
  await expect.poll(() => onboarding.evaluate((element) => element.scrollTop)).toBeGreaterThan(0);
  await expect(page.getByRole("button", { name: /Use Claude Code|Use Codex|Use OpenCode/ })).toBeVisible();
  expect(failures).toEqual([]);
});

test("renders Codex models, approval, plan, and fast mode from discovery", async ({ page }) => {
  const failures = collectBrowserFailures(page);
  const discovery = readyClaudeInventory();
  const control = (id: string, label: string, kind: string, options: { id: string; label: string; is_default: boolean }[]) => ({
    id, label, kind,
    options: options.map((option) => ({ ...option, description: option.label, dangerous: false })),
  });
  const codex = {
    ...discovery.harnesses[0],
    provider: "codex",
    permissions: { ...discovery.harnesses[0].permissions, plan: false, live_approvals: false, live_questions: false },
    control_groups: [
      control("approval_policy", "Approval", "permission", [
        { id: "on-request", label: "On request", is_default: true },
        { id: "never", label: "Never ask", is_default: false },
      ]),
      control("sandbox_mode", "Sandbox", "sandbox", [
        { id: "workspace-write", label: "Workspace write", is_default: true },
        { id: "read-only", label: "Read only", is_default: false },
      ]),
      control("collaboration_mode", "Mode", "collaboration", [
        { id: "default", label: "Work", is_default: true },
        { id: "plan", label: "Plan", is_default: false },
      ]),
    ],
    models: { status: "ready", source: "fixture", models: [{
      id: "gpt-5.6-sol", label: "GPT-5.6 Sol", description: "Codex model", is_default: true,
      reasoning_efforts: ["low", "medium", "high", "xhigh", "max", "ultra"].map((id) => ({
        id, label: ({ low: "Low", medium: "Medium", high: "High", xhigh: "Extra high", max: "Max", ultra: "Ultra" } as Record<string, string>)[id],
        description: id, is_default: id === "low",
      })),
      service_tiers: [{ id: "priority", label: "Fast", description: "Faster response", is_default: false }],
    }], error: null },
  };
  await page.route("**/api/discovery", (route) => route.fulfill({ json: { ...discovery, harnesses: [codex] } }));
  await page.route("**/api/chats?*", (route) => route.fulfill({ json: [] }));
  await page.goto("/");
  await page.getByLabel("Harness").selectOption("codex");

  await expect(page.getByLabel("Model")).toHaveValue("gpt-5.6-sol");
  const thinking = page.getByLabel("Thinking");
  await expect(thinking).toHaveValue("low");
  await expect(thinking.locator("option")).toContainText(["Low", "Medium", "High", "Extra high", "Max", "Ultra"]);
  await expect(thinking.locator("option")).not.toContainText(["Ultra code"]);
  await expect(page.getByLabel("Approval")).toHaveValue("on-request");
  await expect(page.getByLabel("Sandbox")).toHaveValue("workspace-write");
  await expect(page.getByRole("button", { name: "Enable Fast service tier" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Enable plan mode" })).toBeVisible();
  expect(failures).toEqual([]);
});

test("renders concrete Claude generations from discovered model metadata", async ({ page }) => {
  const failures = collectBrowserFailures(page);
  const discovery = readyClaudeInventory();
  discovery.harnesses[0].models.models = [{
    id: "sonnet",
    label: "Sonnet",
    description: "Sonnet 5 · Efficient for routine tasks",
    is_default: true,
    reasoning_efforts: [
      { id: "off", label: "Off", description: "Disable thinking", is_default: false },
      { id: "low", label: "Low", description: "Low thinking", is_default: false },
      { id: "medium", label: "Medium", description: "Medium thinking", is_default: false },
      { id: "high", label: "High", description: "High thinking", is_default: true },
      { id: "xhigh", label: "Extra high", description: "Extra high thinking", is_default: false },
      { id: "max", label: "Max", description: "Maximum thinking", is_default: false },
      { id: "ultracode", label: "Ultra code", description: "Ultra code thinking", is_default: false },
    ],
    service_tiers: [],
  }];
  await page.route("**/api/discovery", (route) => route.fulfill({ json: discovery }));
  await page.route("**/api/chats?*", (route) => route.fulfill({ json: [] }));
  await page.goto("/");
  await page.getByLabel("Harness").selectOption("claude");

  const labels = await page.getByLabel("Model").locator("option").allTextContents();
  expect(labels.some((label) => /^Sonnet \d/.test(label))).toBe(true);
  expect(labels).not.toContain("Claude Sonnet");
  const thinkingLabels = await page.getByLabel("Thinking").locator("option").allTextContents();
  expect(thinkingLabels).toEqual(["Off", "Low", "Medium", "High", "Extra high", "Max", "Ultra code"]);
  expect(thinkingLabels).not.toContain("Ultra");
  expect(failures).toEqual([]);
});

test("streams a persistent chat and restores multiple messages after reload", async ({ page }) => {
  const failures = collectBrowserFailures(page);
  const chatId = await openFixtureChat(page, "Verify streaming and approval persistence.");
  const savedResponse = await page.request.get(`/api/chats/${encodeURIComponent(chatId)}`);
  expect(savedResponse.ok()).toBe(true);
  const savedView = await savedResponse.json() as { chat: { working_directory: string } };
  expect(savedView.chat.working_directory).toMatch(/^\//);

  await expect(page.getByTestId("chat-status")).toHaveText("Approval needed");
  await expect(page.locator(".chat-header-context h1")).toHaveText("Verify streaming and approval persistence.");
  await expect(page.locator(".chat-header-folder")).toHaveText(savedView.chat.working_directory);
  await expect(page.getByTestId("plan-card")).toContainText("Verify the runtime boundary");
  await expect(page.getByTestId("approval-card")).toContainText("cargo test");
  await page.getByRole("button", { name: "Allow command" }).click();

  await expect(page.getByTestId("chat-status")).toHaveText("Succeeded");
  const tool = page.getByTestId("tool-call");
  await expect(tool).toHaveCount(1);
  await expect(tool).toContainText("Parameters");
  await expect(tool).toContainText("cargo test --doc");
  await expect(tool).toContainText("Result");
  await expect(tool).toContainText("Doc tests passed.");
  await expect(page.getByTestId("assistant-message")).toHaveText(
    "The SDK stream is live. Approval completed and the turn finished safely.",
  );
  await expect(page).toHaveURL(/\?chat=chat_\d+/);

  const response = await page.request.post(`/api/chats/${encodeURIComponent(chatId)}/messages`, {
    data: {
      prompt: "Continue this same saved chat.",
      mode: "demo",
      provider: "claude",
      permission: "plan",
      scenario: "approval",
      model: null,
      reasoning: null,
      harness_options: {},
      target: { kind: "local" },
    },
  });
  expect(response.ok()).toBe(true);
  await page.reload();
  await expect(page.getByTestId("chat-status")).toHaveText("Approval needed");
  await page.getByRole("button", { name: "Allow command" }).click();
  await expect(page.getByTestId("chat-status")).toHaveText("Succeeded");
  await expect(page.getByTestId("user-message")).toHaveCount(2);
  await expect(page.getByTestId("assistant-message")).toHaveCount(2);
  await expect(page.getByTestId("tool-call")).toHaveCount(2);

  await page.reload();
  await expect(page.getByTestId("chat-status")).toHaveText("Succeeded");
  await expect(page.getByTestId("user-message")).toHaveCount(2);
  await expect(page.getByTestId("assistant-message")).toHaveCount(2);
  await expect(page.getByTestId("tool-call")).toHaveCount(2);
  await expect(page.getByTestId("activity-list")).toContainText("plan_created");
  await expect(page.getByTestId("activity-list")).toContainText("approval_requested");
  await expect(page.getByTestId("event-age").first()).toHaveAttribute("datetime", /^\d{4}-\d{2}-\d{2}T/);
  await expect(page.getByTestId("event-age").first()).toHaveText(/^(now|\d+[smhd] ago)$/);
  await expect(page.locator(".activity-panel .event-row").last()).toBeInViewport();
  expect(failures).toEqual([]);
});

test("surfaces a provider failure", async ({ page }) => {
  const failures = collectBrowserFailures(page);
  await openFixtureChat(page, "Exercise the typed failure path.", "failure");

  await expect(page.getByTestId("chat-status")).toHaveText("Failed");
  await expect(page.getByRole("alert").filter({ hasText: "Response failed" })).toBeVisible();
  expect(failures).toEqual([]);
});

test("cancels a response while approval is pending", async ({ page }) => {
  const failures = collectBrowserFailures(page);
  await openFixtureChat(page, "Cancel this turn at its approval boundary.");
  await expect(page.getByTestId("chat-status")).toHaveText("Approval needed");
  await page.getByRole("button", { name: "Cancel running turn", exact: true }).click();

  await expect(page.getByTestId("chat-status")).toHaveText("Cancelled");
  expect(failures).toEqual([]);
});

test("exposes saved chats through the mobile drawer", async ({ page }) => {
  const failures = collectBrowserFailures(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await openFixtureChat(page, "Keep this chat visible in the mobile drawer.");

  await page.getByRole("button", { name: "Open chats" }).click();
  const drawer = page.getByRole("dialog", { name: "Chats" });
  await expect(drawer).toBeVisible();
  await expect(drawer.getByRole("button", { name: "Close chats" })).toBeVisible();
  await expect(drawer.getByRole("heading", { name: "Chats", exact: true }).last()).toBeVisible();
  const widths = await page.evaluate(() => ({ viewport: window.innerWidth, document: document.documentElement.scrollWidth }));
  expect(widths.document).toBeLessThanOrEqual(widths.viewport);
  expect(failures).toEqual([]);
});
