import { eq, sql } from "drizzle-orm";
import { randomUUID } from "node:crypto";

import { db as defaultDb } from "../client";
import {
  conversationMessages,
  conversationThreads,
  type ConversationMessage,
  type ConversationThread,
  type NewConversationMessage,
  type NewConversationThread,
} from "../schema";

type Db = typeof defaultDb;

export type CreateThreadInput = Omit<
  NewConversationThread,
  "createdAt" | "threadId" | "updatedAt"
> & {
  threadId?: string;
};

export type AppendMessageInput = Omit<
  NewConversationMessage,
  "createdAt" | "messageId"
> & {
  messageId?: string;
};

export async function createThread(
  input: CreateThreadInput,
  db: Db = defaultDb,
): Promise<ConversationThread> {
  const [thread] = await db
    .insert(conversationThreads)
    .values({ ...input, threadId: input.threadId ?? randomUUID() })
    .returning();

  if (!thread) throw new Error("Failed to create conversation thread");
  return thread;
}

export async function appendMessage(
  input: AppendMessageInput,
  db: Db = defaultDb,
): Promise<ConversationMessage> {
  const [message] = await db
    .insert(conversationMessages)
    .values({ ...input, messageId: input.messageId ?? randomUUID() })
    .returning();

  await db
    .update(conversationThreads)
    .set({ updatedAt: sql`NOW()` })
    .where(eq(conversationThreads.threadId, input.threadId));

  if (!message) throw new Error("Failed to append conversation message");
  return message;
}
