import { getObject, storageLayout } from "../storage/local.js";

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
  '- **Delta-only writes.** System prompt instructs the agent to record distilled facts — "tried lookback=60 top_k=10, Sharpe 1.2, rejected (DD)" — never to dump conversation transcripts.',
  '- **Confirm before writing user profile.** The agent asks before updating `profile.md` ("should I remember you prefer weekly rebalance?"). Strategy memory is scratch — writes freely.',
  "- **Size control by prompt, not by truncation.** When a section grows past the soft cap, the agent is instructed to compress it on its next turn. No auto-truncation — destructive and surprising.",
].join("\n");

export const AGENT_SCRIPT_REGISTRY: readonly AgentScriptDefinition[] = [
  {
    name: "read_memory",
    summary: "Read user or strategy memory from local storage.",
    signature:
      "--scope {user|strategy} --user <user_id> [--strategy <strategy_id>]",
    example:
      "bash agent/scripts/run_agent_script.sh read_memory --scope strategy --user user-123 --strategy strategy-456",
  },
  {
    name: "write_memory",
    summary: "Update a named section in a user or strategy memory file.",
    signature:
      '--scope {user|strategy} --user <user_id> [--strategy <strategy_id>] --section <name> --mode {append|replace} --content "<markdown>"',
    example:
      'bash agent/scripts/run_agent_script.sh write_memory --scope strategy --user user-123 --strategy strategy-456 --section tried --mode append --content "- 2026-04-25: lookback=90, top_k=5 -> Sharpe 0.9"',
    note: "May be unimplemented in some environments.",
  },
  {
    name: "list_universe",
    summary: "List the top-N coins by market cap from the dataset cache.",
    signature: "--top-n <count> [--as-of YYYY-MM-DD]",
    example:
      "bash agent/scripts/run_agent_script.sh list_universe --top-n 50 --as-of 2026-04-25",
  },
  {
    name: "run_backtest",
    summary:
      "Run a JSON-specified backtest and return metrics plus equity curve.",
    signature: "--spec '<json>'",
    example:
      'bash agent/scripts/run_agent_script.sh run_backtest --spec \'{"signal_type":"cross_sectional_momentum","lookback_days":90,"top_k":5,"rebalance_frequency":"weekly"}\'',
  },
  {
    name: "list_runs",
    summary: "List prior runs for a strategy with one-line summaries.",
    signature: "--strategy-id <strategy_id> [--limit <count>]",
    example:
      "bash agent/scripts/run_agent_script.sh list_runs --strategy-id 11111111-1111-1111-1111-111111111111 --limit 10",
  },
] as const;

async function defaultReadSection(key: string): Promise<string | null> {
  const object = await getObject(key);

  return object?.body ?? null;
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
      "Always invoke them with `bash agent/scripts/run_agent_script.sh <script> ...`.",
      "The wrapper applies the per-call timeout, kills hung scripts, and emits `AGENT_SCRIPT_TIMEOUT:` when a script times out.",
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
