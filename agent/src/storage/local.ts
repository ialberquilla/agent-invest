import { createHash, randomUUID } from "node:crypto";
import {
  access,
  mkdir,
  readFile,
  rename,
  rm,
  stat,
  unlink,
  writeFile,
} from "node:fs/promises";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const DEFAULT_TEXT_CONTENT_TYPE = "text/plain; charset=utf-8";
const REPO_ROOT = fileURLToPath(new URL("../../../", import.meta.url));
const DEFAULT_STORAGE_ROOT = resolve(REPO_ROOT, ".data/storage");

export type StorageConfig = {
  root: string;
};

export type StorageObject = {
  root: string;
  key: string;
  path: string;
  body: string;
  etag?: string;
  contentLength?: number;
  contentType?: string;
  lastModified?: Date;
};

export type StorageObjectHead = {
  root: string;
  key: string;
  path: string;
  etag?: string;
  contentLength?: number;
  contentType?: string;
  lastModified?: Date;
};

export type PutObjectOptions = {
  cacheControl?: string;
  contentType?: string;
  metadata?: Record<string, string>;
};

export type PutObjectResult = {
  root: string;
  key: string;
  path: string;
  etag?: string;
  versionId?: string;
};

type ConditionalPutOptions = PutObjectOptions & {
  ifMatch?: string;
  ifNoneMatch?: string;
};

type StoredMetadata = {
  cacheControl?: string;
  contentType?: string;
  metadata?: Record<string, string>;
};

function readOptionalEnv(name: string): string | undefined {
  const value = process.env[name]?.trim();
  return value ? value : undefined;
}

function normalizeKeySegment(value: string, name: string): string {
  const normalized = value.trim();

  if (!normalized) {
    throw new Error(`${name} must not be empty`);
  }

  if (normalized.includes("/")) {
    throw new Error(`${name} must not contain '/'`);
  }

  return normalized;
}

function buildStorageKey(...segments: Array<string | undefined>): string {
  return segments.filter(Boolean).join("/");
}

function normalizeEtag(etag: string | undefined): string | undefined {
  if (!etag) {
    return undefined;
  }

  return etag.replace(/^"|"$/g, "");
}

function createEtag(body: string): string {
  return createHash("sha256").update(body).digest("hex");
}

function metadataPathFor(filePath: string): string {
  return `${filePath}.metadata.json`;
}

function hasMetadata(metadata: StoredMetadata): boolean {
  return Boolean(
    metadata.cacheControl ||
    metadata.contentType ||
    (metadata.metadata && Object.keys(metadata.metadata).length > 0),
  );
}

function isNotFoundError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as { code?: string }).code === "ENOENT"
  );
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await access(filePath);
    return true;
  } catch (error) {
    if (isNotFoundError(error)) {
      return false;
    }

    throw error;
  }
}

async function readStoredMetadata(filePath: string): Promise<StoredMetadata> {
  try {
    const serialized = await readFile(metadataPathFor(filePath), "utf8");
    const parsed = JSON.parse(serialized) as StoredMetadata;

    return {
      cacheControl:
        typeof parsed.cacheControl === "string"
          ? parsed.cacheControl
          : undefined,
      contentType:
        typeof parsed.contentType === "string" ? parsed.contentType : undefined,
      metadata:
        parsed.metadata && typeof parsed.metadata === "object"
          ? parsed.metadata
          : undefined,
    };
  } catch (error) {
    if (isNotFoundError(error) || error instanceof SyntaxError) {
      return {};
    }

    throw error;
  }
}

async function writeStoredMetadata(
  filePath: string,
  metadata: StoredMetadata,
): Promise<void> {
  const path = metadataPathFor(filePath);

  if (!hasMetadata(metadata)) {
    await rm(path, { force: true });
    return;
  }

  await writeFile(path, `${JSON.stringify(metadata, null, 2)}\n`, "utf8");
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

async function readObjectBody(filePath: string): Promise<string> {
  return readFile(filePath, "utf8");
}

function resolveStoragePath(key: string): string {
  const { root } = getStorageConfig();
  const filePath = resolve(root, key);
  const relativePath = relative(root, filePath);

  if (
    relativePath === "" ||
    relativePath === "." ||
    relativePath.startsWith("../") ||
    relativePath === ".."
  ) {
    throw new Error(`Storage key must resolve inside STORAGE_ROOT: ${key}`);
  }

  return filePath;
}

async function putObjectInternal(
  key: string,
  body: string,
  options: ConditionalPutOptions = {},
): Promise<PutObjectResult> {
  const config = getStorageConfig();
  const path = resolveStoragePath(key);
  const existing = await headObject(key);

  if (options.ifNoneMatch === "*" && existing !== null) {
    throw new ConditionalWriteConflictError(key);
  }

  const expectedEtag = normalizeEtag(options.ifMatch);
  if (expectedEtag && existing?.etag !== expectedEtag) {
    throw new ConditionalWriteConflictError(key);
  }

  await writeFileAtomically(path, body);
  await writeStoredMetadata(path, {
    cacheControl: options.cacheControl,
    contentType: options.contentType,
    metadata: options.metadata,
  });

  return {
    root: config.root,
    key,
    path,
    etag: createEtag(body),
    versionId: undefined,
  };
}

export class ConditionalWriteConflictError extends Error {
  readonly key: string;

  constructor(key: string, cause?: unknown) {
    super(`Conditional storage write failed for ${key}`, { cause });
    this.name = "ConditionalWriteConflictError";
    this.key = key;
  }
}

export function getStorageConfig(): StorageConfig {
  const configuredRoot = readOptionalEnv("STORAGE_ROOT");

  return {
    root: configuredRoot
      ? resolve(REPO_ROOT, configuredRoot)
      : DEFAULT_STORAGE_ROOT,
  };
}

export const storageLayout = {
  userPrefix(userId: string): string {
    return buildStorageKey("users", normalizeKeySegment(userId, "userId"));
  },

  userProfileKey(userId: string): string {
    return buildStorageKey(this.userPrefix(userId), "profile.md");
  },

  strategiesPrefix(userId: string): string {
    return buildStorageKey(this.userPrefix(userId), "strategies");
  },

  strategyPrefix(userId: string, strategyId: string): string {
    return buildStorageKey(
      this.strategiesPrefix(userId),
      normalizeKeySegment(strategyId, "strategyId"),
    );
  },

  strategyInstructionsKey(userId: string, strategyId: string): string {
    return buildStorageKey(
      this.strategyPrefix(userId, strategyId),
      "instructions.md",
    );
  },

  strategyMemoryKey(userId: string, strategyId: string): string {
    return buildStorageKey(
      this.strategyPrefix(userId, strategyId),
      "memory.md",
    );
  },

  strategyArtifactsPrefix(userId: string, strategyId: string): string {
    return buildStorageKey(
      this.strategyPrefix(userId, strategyId),
      "artifacts",
    );
  },

  datasetsPrefix(): string {
    return "datasets";
  },

  datasetKey(name: string): string {
    return buildStorageKey(
      this.datasetsPrefix(),
      normalizeKeySegment(name, "name"),
    );
  },
};

export async function getObject(key: string): Promise<StorageObject | null> {
  const config = getStorageConfig();
  const path = resolveStoragePath(key);

  try {
    const [body, fileStat, metadata] = await Promise.all([
      readObjectBody(path),
      stat(path),
      readStoredMetadata(path),
    ]);

    return {
      root: config.root,
      key,
      path,
      body,
      etag: createEtag(body),
      contentLength: fileStat.size,
      contentType: metadata.contentType,
      lastModified: fileStat.mtime,
    };
  } catch (error) {
    if (isNotFoundError(error)) {
      return null;
    }

    throw error;
  }
}

export async function headObject(
  key: string,
): Promise<StorageObjectHead | null> {
  const object = await getObject(key);

  if (object === null) {
    return null;
  }

  return {
    root: object.root,
    key: object.key,
    path: object.path,
    etag: object.etag,
    contentLength: object.contentLength,
    contentType: object.contentType ?? DEFAULT_TEXT_CONTENT_TYPE,
    lastModified: object.lastModified,
  };
}

export async function putObject(
  key: string,
  body: string,
  options: PutObjectOptions = {},
): Promise<PutObjectResult> {
  return putObjectInternal(key, body, options);
}

export async function putObjectIfMatch(
  key: string,
  body: string,
  etag: string,
  options: PutObjectOptions = {},
): Promise<PutObjectResult> {
  return putObjectInternal(key, body, {
    ...options,
    ifMatch: etag,
  });
}

export async function putObjectIfNoneMatch(
  key: string,
  body: string,
  options: PutObjectOptions = {},
): Promise<PutObjectResult> {
  return putObjectInternal(key, body, {
    ...options,
    ifNoneMatch: "*",
  });
}

export async function deleteObject(key: string): Promise<void> {
  const path = resolveStoragePath(key);

  await Promise.all([
    unlink(path).catch((error) => {
      if (!isNotFoundError(error)) {
        throw error;
      }
    }),
    unlink(metadataPathFor(path)).catch((error) => {
      if (!isNotFoundError(error)) {
        throw error;
      }
    }),
  ]);
}
