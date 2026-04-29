import assert from "node:assert/strict";
import { access, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { deleteObject, getObject, putObject, storageLayout } from "./local.js";

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

test("local storage reads, writes, and deletes memory files", async () => {
  await withStorageRoot(async (storageRoot) => {
    const userId = "user-123";
    const strategyId = "strategy-456";
    const memoryKey = storageLayout.strategyMemoryKey(userId, strategyId);
    const initialBody = "## tried\n\n- baseline\n";

    assert.equal(await getObject(memoryKey), null);

    await putObject(memoryKey, initialBody);
    const storedBody = await readFile(join(storageRoot, memoryKey), "utf8");
    assert.equal(storedBody, initialBody);

    const fetched = await getObject(memoryKey);
    assert.equal(fetched, initialBody);

    const updatedBody = `${initialBody}- retry winner\n`;
    await putObject(memoryKey, updatedBody);
    assert.equal(await getObject(memoryKey), updatedBody);

    await assert.rejects(() =>
      access(`${join(storageRoot, memoryKey)}.metadata.json`),
    );

    await deleteObject(memoryKey);
    assert.equal(await getObject(memoryKey), null);
  });
});
