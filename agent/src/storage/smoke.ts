import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import {
  S3ConditionalRequestConflictError,
  describeS3Target,
  ensureStrategyLayout,
  ensureUserLayout,
  formatS3Error,
  getObject,
  headObject,
  putObject,
  putObjectIfMatch,
  putObjectIfNoneMatch,
  s3Layout,
} from "./s3.js";

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

async function main(): Promise<void> {
  const suffix = randomUUID();
  const userId = `smoke-user-${suffix}`;
  const strategyId = `smoke-strategy-${suffix}`;
  const memoryKey = s3Layout.strategyMemoryKey(userId, strategyId);
  const onceOnlyKey = `${s3Layout.strategyArtifactsPrefix(userId, strategyId)}/if-none-match-${suffix}.md`;

  console.info(`Running S3 smoke check against ${describeS3Target()}`);

  await ensureUserLayout(userId);
  await ensureUserLayout(userId);
  await ensureStrategyLayout(userId, strategyId);
  await ensureStrategyLayout(userId, strategyId);

  assert(
    (await headObject(s3Layout.userKeepaliveKey(userId))) !== null,
    "Expected the user prefix marker to exist after ensureUserLayout",
  );

  const initialBody = `# Smoke check\n\n${suffix}\n`;
  await putObject(memoryKey, initialBody, {
    contentType: "text/markdown; charset=utf-8",
  });

  const fetched = await getObject(memoryKey);
  assert(fetched !== null, "Expected getObject to return the uploaded file");
  assert(
    fetched.body === initialBody,
    "Expected getObject to round-trip the markdown body",
  );

  const head = await headObject(memoryKey);
  assert(head?.etag, "Expected headObject to return an ETag");

  const updatedBody = `${initialBody}\nupdated\n`;
  await putObjectIfMatch(memoryKey, updatedBody, head.etag, {
    contentType: "text/markdown; charset=utf-8",
  });

  let sawIfMatchConflict = false;

  try {
    await putObjectIfMatch(memoryKey, `${updatedBody}\nstale\n`, head.etag, {
      contentType: "text/markdown; charset=utf-8",
    });
  } catch (error) {
    if (error instanceof S3ConditionalRequestConflictError) {
      sawIfMatchConflict = true;
    } else {
      throw error;
    }
  }

  assert(
    sawIfMatchConflict,
    "Expected putObjectIfMatch to reject a stale ETag with a typed conflict",
  );

  await putObjectIfNoneMatch(onceOnlyKey, "first write\n", {
    contentType: "text/markdown; charset=utf-8",
  });

  let sawIfNoneMatchConflict = false;

  try {
    await putObjectIfNoneMatch(onceOnlyKey, "second write\n", {
      contentType: "text/markdown; charset=utf-8",
    });
  } catch (error) {
    if (error instanceof S3ConditionalRequestConflictError) {
      sawIfNoneMatchConflict = true;
    } else {
      throw error;
    }
  }

  assert(
    sawIfNoneMatchConflict,
    "Expected putObjectIfNoneMatch to reject an existing key with a typed conflict",
  );

  console.info("S3 smoke check passed.");
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  void main().catch((error) => {
    console.error(formatS3Error(error));
    process.exitCode = 1;
  });
}
