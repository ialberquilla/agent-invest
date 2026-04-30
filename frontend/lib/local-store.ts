export type ChatMessage = {
  role: "user" | "agent";
  text: string;
  run_id?: string;
  status?: string;
  error?: string;
};

const STRATEGY_ID_KEY = "agent-invest:strategy-id";
const MESSAGE_KEY_PREFIX = "agent-invest:messages:";

function canUseLocalStorage() {
  return (
    typeof window !== "undefined" && typeof window.localStorage !== "undefined"
  );
}

function messageKey(strategyId: string) {
  return `${MESSAGE_KEY_PREFIX}${strategyId}`;
}

function isChatMessage(value: unknown): value is ChatMessage {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }

  const message = value as Record<string, unknown>;

  return (
    (message.role === "user" || message.role === "agent") &&
    typeof message.text === "string" &&
    (message.run_id === undefined || typeof message.run_id === "string") &&
    (message.status === undefined || typeof message.status === "string") &&
    (message.error === undefined || typeof message.error === "string")
  );
}

function parseStoredJson<T>(
  key: string,
  isValid: (value: unknown) => value is T,
) {
  if (!canUseLocalStorage()) {
    return null;
  }

  const raw = window.localStorage.getItem(key);
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as unknown;
    return isValid(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

export function getStrategyId() {
  const strategyId = parseStoredJson(
    STRATEGY_ID_KEY,
    (value): value is string =>
      typeof value === "string" && value.trim().length > 0,
  );

  return strategyId ?? null;
}

export function setStrategyId(strategyId: string) {
  if (!canUseLocalStorage()) {
    return;
  }

  window.localStorage.setItem(STRATEGY_ID_KEY, JSON.stringify(strategyId));
}

export function clearStrategyId() {
  if (!canUseLocalStorage()) {
    return;
  }

  window.localStorage.removeItem(STRATEGY_ID_KEY);
}

export function getMessages(strategyId: string) {
  const messages = parseStoredJson(
    messageKey(strategyId),
    (value): value is ChatMessage[] =>
      Array.isArray(value) && value.every((entry) => isChatMessage(entry)),
  );

  return messages ?? [];
}

export function setMessages(strategyId: string, messages: ChatMessage[]) {
  if (!canUseLocalStorage()) {
    return;
  }

  window.localStorage.setItem(messageKey(strategyId), JSON.stringify(messages));
}
