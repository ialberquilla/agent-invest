import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  deleteObject,
  getObject,
  getStorageConfig,
  isStorageDisabled,
  putObject,
  resolveStoragePath,
} from "../src/storage/local.js";

test("AGENT_STORAGE_DISABLED disables local storage reads and writes", async () => {
  const previousStorageRoot = process.env.STORAGE_ROOT;
  const previousStorageDisabled = process.env.AGENT_STORAGE_DISABLED;
  const storageRoot = await mkdtemp(join(tmpdir(), "agent-invest-storage-"));

  process.env.STORAGE_ROOT = storageRoot;
  process.env.AGENT_STORAGE_DISABLED = "1";

  try {
    assert.equal(isStorageDisabled(), true);
    assert.equal(getStorageConfig().disabled, true);
    assert.equal(await getObject("users/user-1/profile.md"), null);
    await putObject("users/user-1/profile.md", "do not persist");
    await deleteObject("users/user-1/profile.md");
    assert.throws(
      () => resolveStoragePath("users/user-1/profile.md"),
      /Storage is disabled/,
    );
    assert.rejects(
      () => readFile(join(storageRoot, "users/user-1/profile.md"), "utf8"),
      (error) =>
        typeof error === "object" &&
        error !== null &&
        "code" in error &&
        error.code === "ENOENT",
    );
  } finally {
    if (previousStorageRoot === undefined) delete process.env.STORAGE_ROOT;
    else process.env.STORAGE_ROOT = previousStorageRoot;
    if (previousStorageDisabled === undefined) {
      delete process.env.AGENT_STORAGE_DISABLED;
    } else {
      process.env.AGENT_STORAGE_DISABLED = previousStorageDisabled;
    }
    await rm(storageRoot, { force: true, recursive: true });
  }
});
