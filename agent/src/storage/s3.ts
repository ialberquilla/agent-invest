import {
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";

const DEFAULT_TEXT_CONTENT_TYPE = "text/plain; charset=utf-8";
const MARKER_OBJECT_NAME = ".keep";

type S3ClientCache = {
  region: string;
  client: S3Client;
};

export type S3Config = {
  bucket: string;
  region: string;
  prefix: string;
};

export type S3Object = {
  bucket: string;
  key: string;
  body: string;
  etag?: string;
  contentLength?: number;
  contentType?: string;
  lastModified?: Date;
};

export type S3ObjectHead = {
  bucket: string;
  key: string;
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
  bucket: string;
  key: string;
  etag?: string;
  versionId?: string;
};

type ConditionalPutOptions = PutObjectOptions & {
  ifMatch?: string;
  ifNoneMatch?: string;
};

type ErrorWithName = {
  name?: string;
  message?: string;
  $metadata?: {
    httpStatusCode?: number;
  };
};

let cachedS3Client: S3ClientCache | undefined;

function readRequiredEnv(name: string): string {
  const value = process.env[name]?.trim();

  if (!value) {
    throw new Error(`Missing required environment variable ${name}`);
  }

  return value;
}

function readOptionalEnv(...names: string[]): string | undefined {
  for (const name of names) {
    const value = process.env[name]?.trim();

    if (value) {
      return value;
    }
  }

  return undefined;
}

function normalizePrefix(prefix: string | undefined): string {
  if (!prefix) {
    return "";
  }

  return prefix.replace(/^\/+|\/+$/g, "");
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

function quoteEtag(etag: string): string {
  return etag.startsWith('"') && etag.endsWith('"') ? etag : `"${etag}"`;
}

function isErrorWithName(error: unknown): error is ErrorWithName {
  return typeof error === "object" && error !== null;
}

function getErrorName(error: unknown): string | undefined {
  return isErrorWithName(error) ? error.name : undefined;
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  return String(error);
}

function getHttpStatusCode(error: unknown): number | undefined {
  return isErrorWithName(error) ? error.$metadata?.httpStatusCode : undefined;
}

function isNotFoundError(error: unknown): boolean {
  const name = getErrorName(error);

  return (
    name === "NoSuchKey" ||
    name === "NotFound" ||
    getHttpStatusCode(error) === 404
  );
}

function isConditionalConflict(error: unknown): boolean {
  const name = getErrorName(error);

  return (
    name === "PreconditionFailed" ||
    name === "ConditionalRequestConflict" ||
    getHttpStatusCode(error) === 412
  );
}

async function getS3Client(): Promise<S3Client> {
  const { region } = getS3Config();

  if (cachedS3Client?.region === region) {
    return cachedS3Client.client;
  }

  const client = new S3Client({ region });
  cachedS3Client = { region, client };

  return client;
}

async function putObjectInternal(
  key: string,
  body: string,
  options: ConditionalPutOptions = {},
): Promise<PutObjectResult> {
  const client = await getS3Client();
  const config = getS3Config();

  try {
    const response = await client.send(
      new PutObjectCommand({
        Bucket: config.bucket,
        Body: body,
        CacheControl: options.cacheControl,
        ContentType: options.contentType ?? DEFAULT_TEXT_CONTENT_TYPE,
        IfMatch: options.ifMatch ? quoteEtag(options.ifMatch) : undefined,
        IfNoneMatch: options.ifNoneMatch,
        Key: key,
        Metadata: options.metadata,
      }),
    );

    return {
      bucket: config.bucket,
      key,
      etag: normalizeEtag(response.ETag),
      versionId: response.VersionId,
    };
  } catch (error) {
    if (options.ifMatch || options.ifNoneMatch) {
      if (isConditionalConflict(error)) {
        throw new S3ConditionalRequestConflictError(key, error);
      }
    }

    throw error;
  }
}

async function ensureMarkerObject(key: string): Promise<void> {
  try {
    await putObjectIfNoneMatch(key, "", {
      contentType: DEFAULT_TEXT_CONTENT_TYPE,
    });
  } catch (error) {
    if (error instanceof S3ConditionalRequestConflictError) {
      return;
    }

    throw error;
  }
}

export class S3ConditionalRequestConflictError extends Error {
  override readonly cause: unknown;
  readonly key: string;

  constructor(key: string, cause?: unknown) {
    super(`Conditional S3 write failed for ${key}`);
    this.name = "S3ConditionalRequestConflictError";
    this.key = key;
    this.cause = cause;
  }
}

export function getS3Config(): S3Config {
  return {
    bucket:
      readOptionalEnv("S3_BUCKET", "AWS_S3_BUCKET") ??
      readRequiredEnv("S3_BUCKET"),
    region:
      readOptionalEnv("AWS_REGION", "AWS_DEFAULT_REGION") ??
      readRequiredEnv("AWS_REGION"),
    prefix: normalizePrefix(readOptionalEnv("S3_PREFIX", "AWS_S3_PREFIX")),
  };
}

export function describeS3Target(): string {
  const { bucket, prefix, region } = getS3Config();
  const scopedBucket = prefix ? `${bucket}/${prefix}` : bucket;

  return `${scopedBucket} (${region})`;
}

export const s3Layout = {
  userPrefix(userId: string): string {
    return buildStorageKey(
      getS3Config().prefix,
      "users",
      normalizeKeySegment(userId, "userId"),
    );
  },

  userKeepaliveKey(userId: string): string {
    return buildStorageKey(this.userPrefix(userId), MARKER_OBJECT_NAME);
  },

  userProfileKey(userId: string): string {
    return buildStorageKey(this.userPrefix(userId), "profile.md");
  },

  strategiesPrefix(userId: string): string {
    return buildStorageKey(this.userPrefix(userId), "strategies");
  },

  strategiesKeepaliveKey(userId: string): string {
    return buildStorageKey(this.strategiesPrefix(userId), MARKER_OBJECT_NAME);
  },

  strategyPrefix(userId: string, strategyId: string): string {
    return buildStorageKey(
      this.strategiesPrefix(userId),
      normalizeKeySegment(strategyId, "strategyId"),
    );
  },

  strategyKeepaliveKey(userId: string, strategyId: string): string {
    return buildStorageKey(
      this.strategyPrefix(userId, strategyId),
      MARKER_OBJECT_NAME,
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

  strategyArtifactsKeepaliveKey(userId: string, strategyId: string): string {
    return buildStorageKey(
      this.strategyArtifactsPrefix(userId, strategyId),
      MARKER_OBJECT_NAME,
    );
  },
};

export async function getObject(key: string): Promise<S3Object | null> {
  const client = await getS3Client();
  const config = getS3Config();

  try {
    const response = await client.send(
      new GetObjectCommand({
        Bucket: config.bucket,
        Key: key,
      }),
    );

    return {
      bucket: config.bucket,
      key,
      body: response.Body ? await response.Body.transformToString() : "",
      etag: normalizeEtag(response.ETag),
      contentLength: response.ContentLength,
      contentType: response.ContentType,
      lastModified: response.LastModified,
    };
  } catch (error) {
    if (isNotFoundError(error)) {
      return null;
    }

    throw error;
  }
}

export async function headObject(key: string): Promise<S3ObjectHead | null> {
  const client = await getS3Client();
  const config = getS3Config();

  try {
    const response = await client.send(
      new HeadObjectCommand({
        Bucket: config.bucket,
        Key: key,
      }),
    );

    return {
      bucket: config.bucket,
      key,
      etag: normalizeEtag(response.ETag),
      contentLength: response.ContentLength,
      contentType: response.ContentType,
      lastModified: response.LastModified,
    };
  } catch (error) {
    if (isNotFoundError(error)) {
      return null;
    }

    throw error;
  }
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

export async function ensureUserLayout(userId: string): Promise<void> {
  await ensureMarkerObject(s3Layout.userKeepaliveKey(userId));
  await ensureMarkerObject(s3Layout.strategiesKeepaliveKey(userId));
}

export async function ensureStrategyLayout(
  userId: string,
  strategyId: string,
): Promise<void> {
  await ensureUserLayout(userId);
  await ensureMarkerObject(s3Layout.strategyKeepaliveKey(userId, strategyId));
  await ensureMarkerObject(
    s3Layout.strategyArtifactsKeepaliveKey(userId, strategyId),
  );
}

export function formatS3Error(error: unknown): string {
  const name = getErrorName(error);
  return name ? `${name}: ${getErrorMessage(error)}` : getErrorMessage(error);
}
