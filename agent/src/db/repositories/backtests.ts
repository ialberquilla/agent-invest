import { randomUUID } from "node:crypto";

import { db as defaultDb } from "../client";
import {
  backtestRequests,
  backtestResults,
  type BacktestRequest,
  type BacktestResult,
  type NewBacktestRequest,
  type NewBacktestResult,
} from "../schema";

type Db = typeof defaultDb;

export type CreateRequestInput = Omit<
  NewBacktestRequest,
  "backtestId" | "createdAt"
> & {
  backtestId?: string;
};

export type StoreResultInput = Omit<NewBacktestResult, "createdAt">;

export async function createRequest(
  input: CreateRequestInput,
  db: Db = defaultDb,
): Promise<BacktestRequest> {
  const [request] = await db
    .insert(backtestRequests)
    .values({ ...input, backtestId: input.backtestId ?? randomUUID() })
    .returning();

  if (!request) throw new Error("Failed to create backtest request");
  return request;
}

export async function storeResult(
  input: StoreResultInput,
  db: Db = defaultDb,
): Promise<BacktestResult> {
  const [result] = await db.insert(backtestResults).values(input).returning();

  if (!result) throw new Error("Failed to store backtest result");
  return result;
}
