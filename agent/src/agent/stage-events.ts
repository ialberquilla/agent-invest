export const STAGE_EVENT_TYPES = {
  started: "stage.started",
  toolCall: "stage.tool_call",
  completed: "stage.completed",
  failed: "stage.failed",
} as const;

export type StageEventType =
  (typeof STAGE_EVENT_TYPES)[keyof typeof STAGE_EVENT_TYPES];
