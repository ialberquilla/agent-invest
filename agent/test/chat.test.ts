import assert from "node:assert/strict";
import test from "node:test";

import {
  CHAT_ALLOWED_TOOLS,
  createChatAgent,
  extractChatResponseText,
  extractPartDelta,
  extractPartUpdate,
  extractTextFromEvent,
} from "../src/agent/chat";

function createRunDbDouble(insertedRuns: unknown[]) {
  return {
    insert() {
      return {
        async values(value: unknown) {
          insertedRuns.push(value);
        },
      };
    },
    update() {
      return {
        set() {
          return {
            where() {
              return Promise.resolve();
            },
          };
        },
      };
    },
  };
}

test("chat agent exposes strategy pipeline, screener, and research tools", () => {
  assert.deepEqual(Object.keys(CHAT_ALLOWED_TOOLS), [
    "agent_invest_run_strategy_pipeline",
    "agent_invest_screen_markets",
    "agent_invest_run_research_code",
  ]);
  assert.deepEqual(CHAT_ALLOWED_TOOLS, {
    agent_invest_run_strategy_pipeline: true,
    agent_invest_screen_markets: true,
    agent_invest_run_research_code: true,
  });
});

test("runStrategyPipeline creates a run and does not await workflow completion", async () => {
  const insertedRuns: unknown[] = [];
  let workflowStarted = false;
  let finishWorkflow!: () => void;
  const workflowFinished = new Promise<void>((resolve) => {
    finishWorkflow = resolve;
  });
  const agent = createChatAgent({
    db: createRunDbDouble(insertedRuns) as never,
    async runWorkflow() {
      workflowStarted = true;
      await workflowFinished;
      return {
        state: {
          run_id: "stub",
          brief: "stub",
          workflow_version: "test",
          attempts: [],
          counters: { reinterpret_brief: 0, broaden_universe: 0 },
        },
      };
    },
  });

  const result = await agent.runStrategyPipeline(
    { brief: "Find a BTC momentum strategy" },
    "chat-1",
  );

  assert.match(result.run_id, /^[0-9a-f-]{36}$/);
  assert.equal(workflowStarted, true);
  assert.deepEqual(insertedRuns, [
    {
      runId: result.run_id,
      threadId: "chat-1",
      kind: "strategy_pipeline",
      status: "running",
      metadata: {
        brief: "Find a BTC momentum strategy",
        based_on_run_id: null,
        overrides: null,
        data_as_of: null,
      },
    },
  ]);

  finishWorkflow();
});

test("runStrategyPipeline forwards rerun overrides to the workflow and run metadata", async () => {
  const insertedRuns: unknown[] = [];
  let forwarded:
    | { brief: unknown; options: unknown }
    | undefined;
  const agent = createChatAgent({
    db: createRunDbDouble(insertedRuns) as never,
    async runWorkflow(_runId, brief, _deps, options) {
      forwarded = { brief, options };
      return {
        state: {
          run_id: "stub",
          brief: "stub",
          workflow_version: "test",
          attempts: [],
          counters: { reinterpret_brief: 0, broaden_universe: 0 },
        },
      };
    },
  });

  const result = await agent.runStrategyPipeline(
    {
      brief: "Find a BTC momentum strategy",
      based_on_run_id: "parent-run",
      overrides: { asset_count_max: 4, max_weight_per_asset: 0.4 },
      data_as_of: "2026-06-01",
    },
    "chat-1",
  );

  // run() is fire-and-forget; give the background workflow a tick to start.
  await new Promise((resolve) => setTimeout(resolve, 0));

  assert.deepEqual(forwarded?.options, {
    based_on_run_id: "parent-run",
    overrides: { asset_count_max: 4, max_weight_per_asset: 0.4 },
    data_as_of: "2026-06-01",
  });
  assert.deepEqual(insertedRuns, [
    {
      runId: result.run_id,
      threadId: "chat-1",
      kind: "strategy_pipeline",
      status: "running",
      metadata: {
        brief: "Find a BTC momentum strategy",
        based_on_run_id: "parent-run",
        overrides: { asset_count_max: 4, max_weight_per_asset: 0.4 },
        data_as_of: "2026-06-01",
      },
    },
  ]);
});

test("extractChatResponseText omits reasoning parts", () => {
  const text = extractChatResponseText({
    parts: [
      { type: "reasoning", text: "I should think this through." },
      { type: "text", text: "Final answer." },
      { type: "tool", state: { output: "{}" } },
    ],
  });

  assert.equal(text, "Final answer.");
});

test("extractTextFromEvent ignores echoed user prompt text", () => {
  const text = extractTextFromEvent(
    {
      type: "message.part.updated",
      properties: {
        part: { id: "part-1", type: "text", text: "what can you do?" },
      },
    },
    "what can you do?",
  );

  assert.equal(text, "");
});

test("extractTextFromEvent keeps assistant text", () => {
  const text = extractTextFromEvent(
    {
      type: "message.part.updated",
      properties: {
        part: { id: "part-2", type: "text", text: "I can help research strategies." },
      },
    },
    "what can you do?",
  );

  assert.equal(text, "I can help research strategies.");
});

test("extractPartDelta reads streaming text tokens", () => {
  const delta = extractPartDelta({
    type: "message.part.delta",
    properties: { partID: "prt_1", field: "text", delta: " world" },
  });

  assert.deepEqual(delta, { partID: "prt_1", field: "text", delta: " world" });
});

test("extractPartDelta ignores non-delta events", () => {
  assert.equal(extractPartDelta({ type: "message.part.updated" }), null);
  assert.equal(
    extractPartDelta({ type: "message.part.delta", properties: {} }),
    null,
  );
});

test("extractPartUpdate reads the part snapshot", () => {
  const update = extractPartUpdate({
    type: "message.part.updated",
    properties: { part: { id: "prt_2", type: "text", text: "Hello world" } },
  });

  assert.deepEqual(update, { id: "prt_2", type: "text", text: "Hello world" });
});

// The reply accumulates many message.part.delta tokens; the trailing
// message.part.updated snapshot only reconciles the part's final text.
test("streaming deltas accumulate into the assistant reply, excluding the prompt", () => {
  const events = [
    {
      type: "message.part.updated",
      properties: { part: { id: "user", type: "text", text: "hi there" } },
    },
    {
      type: "message.part.delta",
      properties: { partID: "asst", field: "text", delta: "Hello" },
    },
    {
      type: "message.part.delta",
      properties: { partID: "asst", field: "text", delta: " world" },
    },
    {
      type: "message.part.updated",
      properties: { part: { id: "asst", type: "text", text: "Hello world" } },
    },
  ];

  const ignored = "hi there";
  const textByPart = new Map<string, string>();
  const ignoredParts = new Set<string>();
  const snapshots: string[] = [];

  for (const event of events) {
    const delta = extractPartDelta(event);
    if (delta && delta.field === "text" && !ignoredParts.has(delta.partID)) {
      textByPart.set(
        delta.partID,
        (textByPart.get(delta.partID) ?? "") + delta.delta,
      );
    }
    const update = extractPartUpdate(event);
    if (update && update.type === "text") {
      if (update.text.trim() === ignored) {
        ignoredParts.add(update.id);
        textByPart.delete(update.id);
      } else {
        textByPart.set(update.id, update.text);
      }
    }
    let reply = "";
    for (const [partId, value] of textByPart) {
      if (ignoredParts.has(partId)) continue;
      reply += value;
    }
    snapshots.push(reply.trim());
  }

  assert.deepEqual(snapshots, ["", "Hello", "Hello world", "Hello world"]);
});
