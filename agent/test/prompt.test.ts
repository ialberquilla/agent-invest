import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  AGENT_SCRIPT_REGISTRY,
  buildSystemPrompt,
  buildToolManifestSection,
  MEMORY_DISCIPLINE_GUIDANCE,
} from "../src/agent/prompt.js";
import { storageLayout } from "../src/storage/local.js";

async function withStorageRoot(callback: () => Promise<void>): Promise<void> {
  const previousStorageRoot = process.env.STORAGE_ROOT;
  const storageRoot = await mkdtemp(join(tmpdir(), "agent-invest-prompt-"));

  process.env.STORAGE_ROOT = storageRoot;

  try {
    await callback();
  } finally {
    if (previousStorageRoot === undefined) {
      delete process.env.STORAGE_ROOT;
    } else {
      process.env.STORAGE_ROOT = previousStorageRoot;
    }

    await rm(storageRoot, { force: true, recursive: true });
  }
}

test("buildSystemPrompt concatenates sections in stable order", async () => {
  await withStorageRoot(async () => {
    const userId = "user-123";
    const strategyId = "strategy-456";
    const seenKeys: string[] = [];
    const values = new Map<string, string | null>([
      [
        storageLayout.userProfileKey(userId),
        "# Preferences\n- prefers weekly rebalance",
      ],
      [
        storageLayout.strategyInstructionsKey(userId, strategyId),
        "Compare weekly and daily rebalances.",
      ],
      [storageLayout.strategyMemoryKey(userId, strategyId), null],
    ]);

    const prompt = await buildSystemPrompt({
      userId,
      strategyId,
      async readSection(key) {
        seenKeys.push(key);
        return values.get(key) ?? null;
      },
    });

    assert.deepEqual(seenKeys, [
      storageLayout.userProfileKey(userId),
      storageLayout.strategyInstructionsKey(userId, strategyId),
      storageLayout.strategyMemoryKey(userId, strategyId),
    ]);
    assert.equal(
      prompt,
      [
        "# User Profile\n# Preferences\n- prefers weekly rebalance",
        "# Strategy Instructions\nCompare weekly and daily rebalances.",
        "# Strategy Memory\n",
        buildToolManifestSection(),
      ].join("\n\n"),
    );
  });
});

test("tool manifest is registry-driven and includes memory guidance", () => {
  const manifest = buildToolManifestSection();

  for (const script of AGENT_SCRIPT_REGISTRY) {
    assert.ok(manifest.includes(`\`${script.name}\``));
    assert.ok(manifest.includes(`Signature: \`${script.signature}\``));
    assert.ok(manifest.includes(`Example: \`${script.example}\``));

    if (script.note) {
      assert.ok(manifest.includes(`Note: ${script.note}`));
    }
  }

  assert.ok(
    manifest.includes(
      "Always invoke them with `bash agent/scripts/run_agent_script.sh <script> ...`.",
    ),
  );
  assert.ok(manifest.includes("AGENT_SCRIPT_TIMEOUT:"));
  assert.ok(manifest.includes("## Memory discipline"));
  assert.ok(manifest.includes(MEMORY_DISCIPLINE_GUIDANCE));
});
