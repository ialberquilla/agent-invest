import { randomUUID } from "node:crypto";

import { db as defaultDb } from "../client";
import { artifacts, type Artifact, type NewArtifact } from "../schema";

type Db = typeof defaultDb;

export type CreateArtifactInput = Omit<
  NewArtifact,
  "artifactId" | "createdAt"
> & {
  artifactId?: string;
};

export async function createArtifact(
  input: CreateArtifactInput,
  db: Db = defaultDb,
): Promise<Artifact> {
  const [artifact] = await db
    .insert(artifacts)
    .values({ ...input, artifactId: input.artifactId ?? randomUUID() })
    .returning();

  if (!artifact) throw new Error("Failed to create artifact");
  return artifact;
}
