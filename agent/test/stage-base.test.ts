import assert from "node:assert/strict";
import test from "node:test";

import {
  buildToolConfig,
  createStageRunner,
  extractToolName,
  type StageDefinition,
} from "../src/agent/stages/base";
import type { ManagedOpencode } from "../src/agent/session";
import type {
  AgentEvent,
  NewAgentEvent,
  NewStageRun,
  StageRun,
} from "../src/db/schema";
import { STAGE_EVENT_TYPES } from "../src/agent/stage-events";

const definition: StageDefinition = {
  name: "thesis",
  allowedTools: ["read", "grep"],
  terminal: "none",
  promptPath: "base-test.md",
};

function createRepositories() {
  const creates: NewStageRun[] = [];
  const events: Array<Omit<NewAgentEvent, "createdAt" | "eventId">> = [];
  const updates: Array<{
    stageRunId: string;
    updates: Partial<Omit<NewStageRun, "stageRunId" | "runId">>;
  }> = [];

  return {
    creates,
    events,
    updates,
    async createStageRun(input: NewStageRun) {
      creates.push(input);
      return input as StageRun;
    },
    async updateStageRun(
      stageRunId: string,
      stageUpdates: Partial<Omit<NewStageRun, "stageRunId" | "runId">>,
    ) {
      updates.push({ stageRunId, updates: stageUpdates });
      return {
        ...creates[0],
        ...stageUpdates,
      } as StageRun;
    },
    async appendEvent(input: Omit<NewAgentEvent, "createdAt" | "eventId">) {
      events.push(input);
      return input as AgentEvent;
    },
  };
}

function createManagedOpencode(
  responses: string[],
  streamEvents: unknown[] = [],
) {
  const prompts: unknown[] = [];
  let responseIndex = 0;

  async function* stream() {
    for (const event of streamEvents) yield event;
  }

  const managed = {
    client: {
      session: {
        async create() {
          return { data: { id: "session-1" } };
        },
        async prompt(input: unknown) {
          prompts.push(input);
          const text = responses[responseIndex] ?? responses.at(-1) ?? "";
          responseIndex += 1;
          return {
            data: {
              info: { usage: { inputTokens: 11, outputTokens: 7 } },
              parts: [{ type: "text", text }],
            },
          };
        },
      },
      event: {
        async subscribe() {
          return { stream: stream() };
        },
      },
    },
    close() {},
  } as unknown as ManagedOpencode;

  return { managed, prompts };
}

test("buildToolConfig enables allowed tools and disables unavailable tools", () => {
  assert.deepEqual(buildToolConfig(["read"], ["read", "grep", "bash"]), {
    read: true,
    grep: false,
    bash: false,
  });
});

test("stage runner persists a successful structured output", async () => {
  const repositories = createRepositories();
  const opencode = createManagedOpencode(['{"ok":true}']);
  const runner = createStageRunner(definition, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    appendEvent: repositories.appendEvent,
    availableTools: ["read", "grep", "bash"],
    env: {},
  });

  const output = await runner.run({ objective: "test" }, "run-1", 1);

  assert.deepEqual(output, { ok: true });
  assert.equal(repositories.creates[0]?.status, "pending");
  assert.equal(repositories.creates[0]?.model, "azure/gpt-5.4");
  assert.equal(repositories.updates[0]?.updates.status, "running");
  assert.equal(repositories.updates.at(-1)?.updates.status, "succeeded");
  assert.deepEqual(repositories.updates.at(-1)?.updates.output, { ok: true });
  assert.equal(repositories.updates.at(-1)?.updates.tokensIn, 11);
  assert.equal(repositories.updates.at(-1)?.updates.tokensOut, 7);
  assert.equal(opencode.prompts.length, 1);
  assert.deepEqual(
    repositories.events.map((event) => event.eventType),
    [
      STAGE_EVENT_TYPES.started,
      "message.part.updated",
      STAGE_EVENT_TYPES.completed,
    ],
  );
  assert.deepEqual(repositories.events[0]?.payload, {
    stage: "thesis",
    round: 1,
    stage_run_id: repositories.creates[0]?.stageRunId,
  });
});

test("stage runner emits tool call events in stage context", async () => {
  const repositories = createRepositories();
  const opencode = createManagedOpencode(
    ['{"ok":true}'],
    [{ type: "tool_call", tool: { name: "grep" } }],
  );
  const runner = createStageRunner(definition, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    appendEvent: repositories.appendEvent,
    availableTools: ["read", "grep", "bash"],
    env: {},
  });

  await runner.run({ objective: "test" }, "run-1", 1);

  assert.deepEqual(
    repositories.events.map((event) => event.eventType),
    [
      STAGE_EVENT_TYPES.started,
      "tool_call",
      STAGE_EVENT_TYPES.toolCall,
      "message.part.updated",
      STAGE_EVENT_TYPES.completed,
    ],
  );
  assert.deepEqual(repositories.events[2]?.payload, {
    stage: "thesis",
    round: 1,
    stage_run_id: repositories.creates[0]?.stageRunId,
    tool_name: "grep",
  });
});

test("stage runner retries once after parse failure and then succeeds", async () => {
  const repositories = createRepositories();
  const opencode = createManagedOpencode(["not json", '{"ok":true}']);
  const runner = createStageRunner(definition, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    appendEvent: repositories.appendEvent,
    availableTools: ["read", "grep", "bash"],
    env: {},
  });

  const output = await runner.run({ objective: "retry" }, "run-1", 1);

  assert.deepEqual(output, { ok: true });
  assert.equal(opencode.prompts.length, 2);
  assert.match(
    JSON.stringify(opencode.prompts[1]),
    /previous response could not be parsed as JSON/,
  );
  assert.equal(repositories.updates.at(-1)?.updates.status, "succeeded");
});

test("stage runner marks the stage run failed after two parse failures", async () => {
  const repositories = createRepositories();
  const opencode = createManagedOpencode(["not json", "still not json"]);
  const runner = createStageRunner(definition, {
    getManagedOpencode: async () => opencode.managed,
    createStageRun: repositories.createStageRun,
    updateStageRun: repositories.updateStageRun,
    appendEvent: repositories.appendEvent,
    availableTools: ["read", "grep", "bash"],
    env: {},
  });

  await assert.rejects(() => runner.run({ objective: "fail" }, "run-1", 1));

  assert.equal(opencode.prompts.length, 2);
  assert.equal(repositories.updates.at(-1)?.updates.status, "failed");
  assert.match(
    repositories.updates.at(-1)?.updates.error ?? "",
    /Unexpected token/,
  );
  assert.deepEqual(
    repositories.events.map((event) => event.eventType),
    [
      STAGE_EVENT_TYPES.started,
      "message.part.updated",
      "message.part.updated",
      STAGE_EVENT_TYPES.failed,
    ],
  );
  assert.match(
    String((repositories.events.at(-1)?.payload as { error?: string }).error),
    /Unexpected token/,
  );
});

test("extractToolName finds nested tool names", () => {
  assert.equal(extractToolName({ event: { tool_name: "read" } }), "read");
  assert.equal(
    extractToolName({
      type: "message.part.updated",
      properties: { part: { type: "tool", tool: "run_candidate_batch" } },
    }),
    "run_candidate_batch",
  );
});
