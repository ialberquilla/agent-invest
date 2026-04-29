import { randomUUID } from "node:crypto";
import {
  mkdir,
  readFile,
  rename,
  rm,
  unlink,
  writeFile,
} from "node:fs/promises";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
const REPO_ROOT = fileURLToPath(new URL("../../../", import.meta.url));
const DEFAULT_STORAGE_ROOT = resolve(REPO_ROOT, ".data/storage");
const readOptionalEnv = (name: string): string | undefined =>
  process.env[name]?.trim() || undefined;
const buildStorageKey = (...segments: Array<string | undefined>): string =>
  segments.filter(Boolean).join("/");
const isNotFoundError = (error: unknown): boolean =>
  typeof error === "object" &&
  error !== null &&
  "code" in error &&
  (error as { code?: string }).code === "ENOENT";
function normalizeKeySegment(value: string, name: string): string {
  const normalized = value.trim();
  if (!normalized) throw new Error(`${name} must not be empty`);
  if (normalized.includes("/")) throw new Error(`${name} must not contain '/'`);
  return normalized;
}
async function writeFileAtomically(
  filePath: string,
  body: string,
): Promise<void> {
  await mkdir(dirname(filePath), { recursive: true });
  const tempPath = `${filePath}.tmp-${randomUUID()}`;
  try {
    await writeFile(tempPath, body, "utf8");
    await rename(tempPath, filePath);
  } finally {
    await rm(tempPath, { force: true });
  }
}
export function getStorageConfig() {
  const configuredRoot = readOptionalEnv("STORAGE_ROOT");
  return {
    root: configuredRoot
      ? resolve(REPO_ROOT, configuredRoot)
      : DEFAULT_STORAGE_ROOT,
  };
}
export const storageLayout = {
  userPrefix: (userId: string) =>
    buildStorageKey("users", normalizeKeySegment(userId, "userId")),
  userProfileKey(userId: string) {
    return buildStorageKey(this.userPrefix(userId), "profile.md");
  },
  strategiesPrefix(userId: string) {
    return buildStorageKey(this.userPrefix(userId), "strategies");
  },
  strategyPrefix(userId: string, strategyId: string) {
    return buildStorageKey(
      this.strategiesPrefix(userId),
      normalizeKeySegment(strategyId, "strategyId"),
    );
  },
  strategyInstructionsKey(userId: string, strategyId: string) {
    return buildStorageKey(
      this.strategyPrefix(userId, strategyId),
      "instructions.md",
    );
  },
  strategyMemoryKey(userId: string, strategyId: string) {
    return buildStorageKey(
      this.strategyPrefix(userId, strategyId),
      "memory.md",
    );
  },
  strategyArtifactsPrefix(userId: string, strategyId: string) {
    return buildStorageKey(
      this.strategyPrefix(userId, strategyId),
      "artifacts",
    );
  },
  datasetsPrefix: () => "datasets",
  datasetKey(name: string) {
    return buildStorageKey(
      this.datasetsPrefix(),
      normalizeKeySegment(name, "name"),
    );
  },
};
export function resolveStoragePath(key: string): string {
  const { root } = getStorageConfig();
  const filePath = resolve(root, key);
  const relativePath = relative(root, filePath);
  if (
    relativePath === "" ||
    relativePath === "." ||
    relativePath === ".." ||
    relativePath.startsWith("../")
  )
    throw new Error(`Storage key must resolve inside STORAGE_ROOT: ${key}`);
  return filePath;
}
export async function getObject(key: string): Promise<string | null> {
  try {
    return await readFile(resolveStoragePath(key), "utf8");
  } catch (error) {
    if (isNotFoundError(error)) return null;
    throw error;
  }
}
export async function putObject(key: string, body: string): Promise<void> {
  await writeFileAtomically(resolveStoragePath(key), body);
}
export async function deleteObject(key: string): Promise<void> {
  await unlink(resolveStoragePath(key)).catch((error) => {
    if (!isNotFoundError(error)) throw error;
  });
}
