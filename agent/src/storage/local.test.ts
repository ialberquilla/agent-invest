import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  ConditionalWriteConflictError,
  getObject,
  headObject,
  putObject,
  putObjectIfMatch,
  putObjectIfNoneMatch,
  storageLayout,
} from "./local.js";

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

async function withStorageRoot(
  callback: (storageRoot: string) => Promise<void>,
) {
  const previousStorageRoot = process.env.STORAGE_ROOT;
  const storageRoot = await mkdtemp(join(tmpdir(), "agent-invest-storage-"));

  process.env.STORAGE_ROOT = storageRoot;

  try {
    await callback(storageRoot);
  } finally {
    if (previousStorageRoot === undefined) {
      delete process.env.STORAGE_ROOT;
    } else {
      process.env.STORAGE_ROOT = previousStorageRoot;
    }

    await rm(storageRoot, { force: true, recursive: true });
  }
}

test("local storage reads, heads, and conditionally writes memory files", async () => {
  await withStorageRoot(async (storageRoot) => {
    const userId = "user-123";
    const strategyId = "strategy-456";
    const memoryKey = storageLayout.strategyMemoryKey(userId, strategyId);
    const onceOnlyKey = `${storageLayout.strategyArtifactsPrefix(userId, strategyId)}/output.json`;
    const initialBody = "## tried\n\n- baseline\n";

    const created = await putObject(memoryKey, initialBody, {
      contentType: "text/markdown; charset=utf-8",
    });
    assert.equal(created.etag, sha256(initialBody));

    const storedBody = await readFile(join(storageRoot, memoryKey), "utf8");
    assert.equal(storedBody, initialBody);

    const fetched = await getObject(memoryKey);
    assert.equal(fetched?.body, initialBody);
    assert.equal(fetched?.etag, sha256(initialBody));
    assert.equal(fetched?.contentType, "text/markdown; charset=utf-8");

    const head = await headObject(memoryKey);
    assert.equal(head?.etag, sha256(initialBody));
    assert.equal(head?.contentType, "text/markdown; charset=utf-8");

    const updatedBody = `${initialBody}- retry winner\n`;
    const updated = await putObjectIfMatch(
      memoryKey,
      updatedBody,
      head!.etag!,
      {
        contentType: "text/markdown; charset=utf-8",
      },
    );
    assert.equal(updated.etag, sha256(updatedBody));

    await assert.rejects(
      () => putObjectIfMatch(memoryKey, `${updatedBody}- stale\n`, head!.etag!),
      (error: unknown) => error instanceof ConditionalWriteConflictError,
    );

    await putObjectIfNoneMatch(onceOnlyKey, '{"ok":true}\n', {
      contentType: "application/json",
    });
    await assert.rejects(
      () => putObjectIfNoneMatch(onceOnlyKey, '{"ok":false}\n'),
      (error: unknown) => error instanceof ConditionalWriteConflictError,
    );
  });
});
