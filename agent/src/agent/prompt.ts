import { getObject, storageLayout } from "../storage/local";

type PromptSectionReader = (key: string) => Promise<string | null>;

export type AgentScriptDefinition = {
  name: string;
  summary: string;
  signature: string;
  example: string;
  note?: string;
};

export type BuildSystemPromptOptions = {
  userId: string;
  strategyId: string;
  readSection?: PromptSectionReader;
};

export const MEMORY_DISCIPLINE_GUIDANCE = [
  "- Use opencode's built-in shell and file-edit tools for memory I/O. Read or update `users/<user_id>/profile.md` and `users/<user_id>/strategies/<strategy_id>/memory.md` under `STORAGE_ROOT` with `cat`, `tee`, or direct file edits instead of calling a dedicated memory script.",
  "- Ask before changing `profile.md`. Strategy `memory.md` is working memory, so keep it concise and update it freely when it helps future turns.",
].join("\n");

export const AGENT_SCRIPT_REGISTRY: readonly AgentScriptDefinition[] = [
  {
    name: "list_universe",
    summary: "List the top-N coins by market cap from the dataset cache.",
    signature: "--top-n <count> [--as-of YYYY-MM-DD]",
    example:
      "uv run --project agent/scripts python -m agent_invest_scripts.list_universe --top-n 50 --as-of 2026-04-25",
  },
  {
    name: "run_backtest",
    summary:
      "Run a JSON-specified backtest and return metrics plus equity curve.",
    signature: "--spec '<json>'",
    example:
      'uv run --project agent/scripts python -m agent_invest_scripts.run_backtest --spec \'{"signal_type":"cross_sectional_momentum","lookback_days":90,"top_k":5,"rebalance_frequency":"weekly"}\'',
  },
  {
    name: "list_runs",
    summary: "List prior runs for a strategy with one-line summaries.",
    signature: "--strategy-id <strategy_id> [--limit <count>]",
    example:
      "uv run --project agent/scripts python -m agent_invest_scripts.list_runs --strategy-id 11111111-1111-1111-1111-111111111111 --limit 10",
  },
] as const;

async function defaultReadSection(key: string): Promise<string | null> {
  return getObject(key);
}

function renderSection(title: string, body: string): string {
  return `# ${title}\n${body}`;
}

function renderScriptDefinition(script: AgentScriptDefinition): string {
  const lines = [
    `- \`${script.name}\``,
    `  Purpose: ${script.summary}`,
    `  Signature: \`${script.signature}\``,
    `  Example: \`${script.example}\``,
  ];

  if (script.note) {
    lines.push(`  Note: ${script.note}`);
  }

  return lines.join("\n");
}

export function buildToolManifestSection(): string {
  const scriptEntries = AGENT_SCRIPT_REGISTRY.map(renderScriptDefinition).join(
    "\n\n",
  );

  return renderSection(
    "Tool Manifest",
    [
      "All agent-facing Python scripts live under `agent/scripts/agent_invest_scripts/`.",
      "Always invoke them with `uv run --project agent/scripts python -m agent_invest_scripts.<script> ...`.",
      "Each script enforces its own per-call timeout and exits non-zero when it times out.",
      "Each script prints structured JSON to stdout, logs to stderr only, and exits non-zero on error.",
      "If a script times out, stop and surface that failure instead of retrying the same command.",
      "",
      "## Scripts",
      scriptEntries,
      "",
      "## Memory discipline",
      MEMORY_DISCIPLINE_GUIDANCE,
    ].join("\n"),
  );
}

export async function buildSystemPrompt({
  userId,
  strategyId,
  readSection = defaultReadSection,
}: BuildSystemPromptOptions): Promise<string> {
  const [profile, instructions, memory] = await Promise.all([
    readSection(storageLayout.userProfileKey(userId)),
    readSection(storageLayout.strategyInstructionsKey(userId, strategyId)),
    readSection(storageLayout.strategyMemoryKey(userId, strategyId)),
  ]);

  return [
    renderSection("User Profile", profile ?? ""),
    renderSection("Strategy Instructions", instructions ?? ""),
    renderSection("Strategy Memory", memory ?? ""),
    buildToolManifestSection(),
  ].join("\n\n");
}
