import assert from "node:assert/strict";
import { access, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  deleteObject,
  getObject,
  putObject,
  storageLayout,
} from "../src/storage/local";

async function withStorageRoot(
  callback: (storageRoot: string) => Promise<void>,
) {
  const previousStorageRoot = process.env.STORAGE_ROOT;
  const storageRoot = await mkdtemp(join(tmpdir(), "pond3r-portfolio-storage-"));

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

test("local storage reads, writes, and deletes objects", async () => {
  await withStorageRoot(async (storageRoot) => {
    const objectKey = storageLayout.datasetKey("sample.json");
    const initialBody = "## tried\n\n- baseline\n";

    assert.equal(await getObject(objectKey), null);

    await putObject(objectKey, initialBody);
    const storedBody = await readFile(join(storageRoot, objectKey), "utf8");
    assert.equal(storedBody, initialBody);

    const fetched = await getObject(objectKey);
    assert.equal(fetched, initialBody);

    const updatedBody = `${initialBody}- retry winner\n`;
    await putObject(objectKey, updatedBody);
    assert.equal(await getObject(objectKey), updatedBody);

    await assert.rejects(() =>
      access(`${join(storageRoot, objectKey)}.metadata.json`),
    );

    await deleteObject(objectKey);
    assert.equal(await getObject(objectKey), null);
  });
});
