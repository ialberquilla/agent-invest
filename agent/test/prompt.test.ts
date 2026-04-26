import assert from "node:assert/strict";
import test from "node:test";

import {
  AGENT_SCRIPT_REGISTRY,
  buildSystemPrompt,
  buildToolManifestSection,
  MEMORY_DISCIPLINE_GUIDANCE,
} from "../src/agent/prompt.js";
import { s3Layout } from "../src/storage/s3.js";

function withS3Env(callback: () => Promise<void>): Promise<void> {
  const previousBucket = process.env.S3_BUCKET;
  const previousRegion = process.env.AWS_REGION;
  const previousPrefix = process.env.S3_PREFIX;

  process.env.S3_BUCKET = "test-bucket";
  process.env.AWS_REGION = "us-east-1";
  delete process.env.S3_PREFIX;

  return callback().finally(() => {
    if (previousBucket === undefined) {
      delete process.env.S3_BUCKET;
    } else {
      process.env.S3_BUCKET = previousBucket;
    }

    if (previousRegion === undefined) {
      delete process.env.AWS_REGION;
    } else {
      process.env.AWS_REGION = previousRegion;
    }

    if (previousPrefix === undefined) {
      delete process.env.S3_PREFIX;
    } else {
      process.env.S3_PREFIX = previousPrefix;
    }
  });
}

test("buildSystemPrompt concatenates sections in stable order", async () => {
  await withS3Env(async () => {
    const userId = "user-123";
    const strategyId = "strategy-456";
    const seenKeys: string[] = [];
    const values = new Map<string, string | null>([
      [
        s3Layout.userProfileKey(userId),
        "# Preferences\n- prefers weekly rebalance",
      ],
      [
        s3Layout.strategyInstructionsKey(userId, strategyId),
        "Compare weekly and daily rebalances.",
      ],
      [s3Layout.strategyMemoryKey(userId, strategyId), null],
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
      s3Layout.userProfileKey(userId),
      s3Layout.strategyInstructionsKey(userId, strategyId),
      s3Layout.strategyMemoryKey(userId, strategyId),
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
