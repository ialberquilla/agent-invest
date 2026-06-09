import type { ArtifactRef, ScreenerResult, StructuredChatResult } from "@/lib/types";

export type ChatMessage = {
  role: "user" | "agent";
  text: string;
  run_id?: string;
  status?: string;
  error?: string;
  artifacts?: ArtifactRef[];
  structured_result?: StructuredChatResult | null;
};

export type KnownStrategy = {
  strategy_id: string;
  label: string;
  created_at: string;
};

export type DeployedStrategy = {
  mandate_id: string;
  chain_id: number;
  vault_address: string;
  asset_address: string;
  status: string;
  label: string;
  updated_at: string;
};

const STRATEGY_ID_KEY = "agent-invest:strategy-id";
const KNOWN_STRATEGIES_KEY = "agent-invest:known-strategies";
const ANONYMOUS_USER_ID_KEY = "agent-invest:anonymous-user-id";
const MESSAGE_KEY_PREFIX = "agent-invest:messages:";
const PINNED_SCREENERS_KEY = "agent-invest:pinned-screeners";
const DEPLOYED_STRATEGIES_KEY = "agent-invest:deployed-strategies";
const STRATEGY_LABEL_MAX_LENGTH = 40;

export type PinnedScreener = {
  id: string;
  label: string;
  definition: ScreenerResult["definition"];
  updated_at: string;
};

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

function isArtifactRef(value: unknown): value is ArtifactRef {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const artifact = value as Record<string, unknown>;
  return typeof artifact.kind === "string" && typeof artifact.path === "string";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isChatMessage(value: unknown): value is ChatMessage {
  if (!isRecord(value)) {
    return false;
  }

  const message = value;

  return (
    (message.role === "user" || message.role === "agent") &&
    typeof message.text === "string" &&
    (message.run_id === undefined || typeof message.run_id === "string") &&
    (message.status === undefined || typeof message.status === "string") &&
    (message.error === undefined || typeof message.error === "string") &&
    (message.artifacts === undefined ||
      (Array.isArray(message.artifacts) &&
        message.artifacts.every(isArtifactRef))) &&
    (message.structured_result === undefined ||
      message.structured_result === null ||
      isRecord(message.structured_result))
  );
}

function isKnownStrategy(value: unknown): value is KnownStrategy {
  if (!isRecord(value)) {
    return false;
  }

  const strategy = value;

  return (
    isNonEmptyString(strategy.strategy_id) &&
    isNonEmptyString(strategy.label) &&
    isNonEmptyString(strategy.created_at)
  );
}

function isDeployedStrategy(value: unknown): value is DeployedStrategy {
  if (!isRecord(value)) return false;
  return (
    isNonEmptyString(value.mandate_id) &&
    typeof value.chain_id === "number" &&
    isNonEmptyString(value.vault_address) &&
    isNonEmptyString(value.asset_address) &&
    isNonEmptyString(value.status) &&
    isNonEmptyString(value.label) &&
    isNonEmptyString(value.updated_at)
  );
}

function isPinnedScreener(value: unknown): value is PinnedScreener {
  if (!isRecord(value) || !isRecord(value.definition)) return false;
  const definition = value.definition;
  return (
    isNonEmptyString(value.id) &&
    isNonEmptyString(value.label) &&
    isNonEmptyString(value.updated_at) &&
    (definition.factor === "momentum" ||
      definition.factor === "risk_adjusted" ||
      definition.factor === "low_volatility") &&
    typeof definition.limit === "number" &&
    typeof definition.gmx_only === "boolean"
  );
}

export function screenerId(definition: ScreenerResult["definition"]) {
  return [
    definition.factor,
    definition.limit,
    definition.gmx_only ? "gmx" : "all",
    definition.as_of ?? "latest",
  ].join(":");
}

function writePinnedScreeners(screeners: PinnedScreener[]) {
  if (!canUseLocalStorage()) return;
  window.localStorage.setItem(PINNED_SCREENERS_KEY, JSON.stringify(screeners));
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

export function getAnonymousUserId() {
  const userId = parseStoredJson(
    ANONYMOUS_USER_ID_KEY,
    (value): value is string =>
      isNonEmptyString(value) && value.startsWith("anon:"),
  );

  if (userId) return userId;
  if (!canUseLocalStorage()) return null;

  const next = `anon:${crypto.randomUUID()}`;
  window.localStorage.setItem(ANONYMOUS_USER_ID_KEY, JSON.stringify(next));
  return next;
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

export function removeKnownStrategy(strategyId: string) {
  const normalizedStrategyId = strategyId.trim();
  if (!normalizedStrategyId) {
    return getKnownStrategies();
  }

  const next = getKnownStrategies().filter(
    (strategy) => strategy.strategy_id !== normalizedStrategyId,
  );
  writeKnownStrategies(next);

  if (canUseLocalStorage()) {
    window.localStorage.removeItem(messageKey(normalizedStrategyId));
  }

  return next;
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

export function getPinnedScreeners() {
  const screeners = parseStoredJson(
    PINNED_SCREENERS_KEY,
    (value): value is PinnedScreener[] =>
      Array.isArray(value) && value.every((entry) => isPinnedScreener(entry)),
  );
  return screeners ?? [];
}

export function getDeployedStrategies() {
  const strategies = parseStoredJson(
    DEPLOYED_STRATEGIES_KEY,
    (value): value is DeployedStrategy[] =>
      Array.isArray(value) && value.every((entry) => isDeployedStrategy(entry)),
  );
  return strategies ?? [];
}

export function upsertDeployedStrategy(strategy: {
  mandate_id: string;
  chain_id: number;
  vault_address: string;
  asset_address: string;
  status: string;
  label?: string;
}) {
  if (!canUseLocalStorage()) return;
  const current = getDeployedStrategies();
  const next: DeployedStrategy = {
    ...strategy,
    label:
      normalizeKnownStrategyLabel(strategy.label) ||
      `Vault ${strategy.vault_address.slice(0, 6)}...${strategy.vault_address.slice(-4)}`,
    updated_at: new Date().toISOString(),
  };
  window.localStorage.setItem(
    DEPLOYED_STRATEGIES_KEY,
    JSON.stringify([
      next,
      ...current.filter((entry) => entry.mandate_id !== strategy.mandate_id),
    ]),
  );
}

export function isScreenerPinned(definition: ScreenerResult["definition"]) {
  const id = screenerId(definition);
  return getPinnedScreeners().some((screener) => screener.id === id);
}

export function pinScreener(result: ScreenerResult, label = result.title) {
  const id = screenerId(result.definition);
  const screeners = getPinnedScreeners();
  const current = screeners.filter((screener) => screener.id !== id);
  const existing = screeners.find((screener) => screener.id === id);
  const normalizedLabel = normalizeText(label) || existing?.label || result.title;
  const pinned: PinnedScreener = {
    id,
    label: normalizedLabel,
    definition: result.definition,
    updated_at: new Date().toISOString(),
  };
  writePinnedScreeners([pinned, ...current]);
}

export function unpinScreener(definition: ScreenerResult["definition"]) {
  const id = screenerId(definition);
  writePinnedScreeners(
    getPinnedScreeners().filter((screener) => screener.id !== id),
  );
}
