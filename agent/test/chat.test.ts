import assert from "node:assert/strict";
import test from "node:test";

import {
  CHAT_ALLOWED_TOOLS,
  createChatAgent,
  extractChatResponseText,
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

test("chat agent exposes exactly run_strategy_pipeline", () => {
  assert.deepEqual(Object.keys(CHAT_ALLOWED_TOOLS), [
    "agent_invest_run_strategy_pipeline",
  ]);
  assert.deepEqual(CHAT_ALLOWED_TOOLS, {
    agent_invest_run_strategy_pipeline: true,
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
      metadata: { brief: "Find a BTC momentum strategy" },
    },
  ]);

  finishWorkflow();
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
