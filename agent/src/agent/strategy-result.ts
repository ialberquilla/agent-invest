export type StrategyResult = Record<string, unknown> & {
  title: string;
  summary: string;
  reasoning: string;
  allocation: unknown[];
  kpis: Record<string, unknown>;
  assumptions: unknown[];
  risks: unknown[];
  next_steps: unknown[];
};

const STRATEGY_RESULT_BLOCK = /```strategy_result\s*\n([\s\S]*?)\n```/i;

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function isValidStrategyResult(value: unknown): value is StrategyResult {
  if (!isRecord(value)) return false;

  return (
    typeof value.title === "string" &&
    typeof value.summary === "string" &&
    typeof value.reasoning === "string" &&
    Array.isArray(value.allocation) &&
    isRecord(value.kpis) &&
    Array.isArray(value.assumptions) &&
    Array.isArray(value.risks) &&
    Array.isArray(value.next_steps)
  );
}

export function parseStrategyResultBlock(text: string): StrategyResult | null {
  const match = text.match(STRATEGY_RESULT_BLOCK);
  if (!match) return null;

  try {
    const parsed: unknown = JSON.parse(match[1]);
    return isValidStrategyResult(parsed) ? parsed : null;
  } catch {
    return null;
  }
}
