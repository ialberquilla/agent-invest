// Structured step logger. One JSON line per event with a consistent
// schema so logs can be filtered by run_id, step, and phase, and so
// the "what didn't happen" coverage view can be derived later.

import pino from "pino";

import type { StepName } from "./state";

const baseLogger = pino({
  level: process.env.LOG_LEVEL ?? "info",
  base: undefined,
  formatters: {
    level(label) {
      return { level: label };
    },
  },
});

export type StepPhase =
  | "enter"
  | "llm_request"
  | "llm_response"
  | "exit"
  | "error"
  | "cap_hit";

export type StepLogContext = {
  run_id: string;
  step: StepName;
  attempt?: number | null;
};

export type StepLogger = {
  enter(payload?: Record<string, unknown>): void;
  exit(
    next: StepName | "complete",
    payload?: Record<string, unknown>,
  ): void;
  llmRequest(payload: { model?: string; prompt_chars?: number }): void;
  llmResponse(payload: {
    tokens_in?: number | null;
    tokens_out?: number | null;
    response_chars?: number;
  }): void;
  error(error: unknown, payload?: Record<string, unknown>): void;
  capHit(payload: {
    cap: string;
    observed?: number;
    limit?: number;
  }): void;
};

export function createStepLogger(context: StepLogContext): StepLogger {
  const start = Date.now();
  const base = baseLogger.child({
    run_id: context.run_id,
    step: context.step,
    attempt: context.attempt ?? null,
  });

  return {
    enter(payload) {
      base.info({ phase: "enter", ...(payload ?? {}) });
    },
    exit(next, payload) {
      base.info({
        phase: "exit",
        next,
        duration_ms: Date.now() - start,
        ...(payload ?? {}),
      });
    },
    llmRequest(payload) {
      base.info({ phase: "llm_request", ...payload });
    },
    llmResponse(payload) {
      base.info({ phase: "llm_response", ...payload });
    },
    error(error, payload) {
      base.error({
        phase: "error",
        message: error instanceof Error ? error.message : String(error),
        ...(payload ?? {}),
      });
    },
    capHit(payload) {
      base.warn({ phase: "cap_hit", ...payload });
    },
  };
}

// Silent logger for tests / contexts where stdout output is unwanted.
export function createSilentStepLogger(): StepLogger {
  return {
    enter() {},
    exit() {},
    llmRequest() {},
    llmResponse() {},
    error() {},
    capHit() {},
  };
}
