export type ChatMessage = {
  role: "user" | "agent";
  text: string;
  run_id?: string;
  status?: string;
  error?: string;
};

export type KnownStrategy = {
  strategy_id: string;
  label: string;
  created_at: string;
};

const STRATEGY_ID_KEY = "agent-invest:strategy-id";
const KNOWN_STRATEGIES_KEY = "agent-invest:known-strategies";
const MESSAGE_KEY_PREFIX = "agent-invest:messages:";
const STRATEGY_LABEL_MAX_LENGTH = 40;

export const EMPTY_STRATEGY_LABEL = "(empty)";

function canUseLocalStorage() {
  return (
    typeof window !== "undefined" && typeof window.localStorage !== "undefined"
  );
}

function messageKey(strategyId: string) {
  return `${MESSAGE_KEY_PREFIX}${strategyId}`;
}

function normalizeText(value: string) {
  return value.trim().replace(/\s+/g, " ");
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
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

function isKnownStrategy(value: unknown): value is KnownStrategy {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }

  const strategy = value as Record<string, unknown>;

  return (
    isNonEmptyString(strategy.strategy_id) &&
    isNonEmptyString(strategy.label) &&
    isNonEmptyString(strategy.created_at)
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

function normalizeKnownStrategyLabel(label: string | undefined) {
  if (typeof label !== "string") {
    return EMPTY_STRATEGY_LABEL;
  }

  const normalized = normalizeText(label);
  return normalized || EMPTY_STRATEGY_LABEL;
}

function writeKnownStrategies(strategies: KnownStrategy[]) {
  if (!canUseLocalStorage()) {
    return;
  }

  window.localStorage.setItem(KNOWN_STRATEGIES_KEY, JSON.stringify(strategies));
}

export function deriveStrategyLabel(text: string) {
  const normalized = normalizeText(text);
  if (!normalized) {
    return EMPTY_STRATEGY_LABEL;
  }

  if (normalized.length <= STRATEGY_LABEL_MAX_LENGTH) {
    return normalized;
  }

  return `${normalized.slice(0, STRATEGY_LABEL_MAX_LENGTH - 3).trimEnd()}...`;
}

export function getStrategyId() {
  const strategyId = parseStoredJson(
    STRATEGY_ID_KEY,
    (value): value is string => isNonEmptyString(value),
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

export function getKnownStrategies() {
  const strategies = parseStoredJson(
    KNOWN_STRATEGIES_KEY,
    (value): value is KnownStrategy[] =>
      Array.isArray(value) && value.every((entry) => isKnownStrategy(entry)),
  );

  return strategies ?? [];
}

export function ensureKnownStrategy(
  strategyId: string,
  createdAt = new Date().toISOString(),
) {
  upsertKnownStrategy({
    strategy_id: strategyId,
    created_at: createdAt,
  });
}

export function upsertKnownStrategy(strategy: {
  strategy_id: string;
  label?: string;
  created_at?: string;
}) {
  const strategyId = strategy.strategy_id.trim();
  if (!strategyId) {
    return;
  }

  const current = getKnownStrategies();
  const existing = current.find((entry) => entry.strategy_id === strategyId);
  const next: KnownStrategy = {
    strategy_id: strategyId,
    label:
      strategy.label !== undefined
        ? normalizeKnownStrategyLabel(strategy.label)
        : (existing?.label ?? EMPTY_STRATEGY_LABEL),
    created_at:
      strategy.created_at ?? existing?.created_at ?? new Date().toISOString(),
  };

  if (existing) {
    writeKnownStrategies(
      current.map((entry) => (entry.strategy_id === strategyId ? next : entry)),
    );
    return;
  }

  writeKnownStrategies([next, ...current]);
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
