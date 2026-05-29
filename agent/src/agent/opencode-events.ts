// Small helpers for parsing opencode SDK events. Used by the chat
// agent to extract tool-call rows for `agent_tool_calls`. Previously
// these lived inside strategist.ts; they outlived the strategist and
// belong as a standalone utility.

export type ToolPartLike = {
  callID: string;
  tool: string;
  state?: {
    status?: string;
    input?: unknown;
    output?: unknown;
    error?: unknown;
  };
};

// Map a bash invocation of `./agent-invest <subcmd>` back to a
// logical tool name ("agent-invest:<subcmd>") so the DB is queryable
// per CLI subcommand, not just per opencode tool.
export function effectiveToolName(toolName: string, input: unknown): string {
  if (toolName !== "bash" || !input || typeof input !== "object") {
    return toolName;
  }
  const command = (input as Record<string, unknown>).command;
  if (typeof command !== "string") return toolName;
  const match = command.match(
    /(?:^|[\s/])(?:\.\/)?agent-invest\s+([a-z_][a-z0-9_-]*)/i,
  );
  return match?.[1] ? `agent-invest:${match[1]}` : toolName;
}

// Walk an opencode event payload to find an embedded "tool" message
// part. Returns the part itself or null.
export function extractToolPart(event: unknown): ToolPartLike | null {
  if (!event || typeof event !== "object") return null;

  const queue: Array<Record<string, unknown>> = [
    event as Record<string, unknown>,
  ];
  const seen = new Set<unknown>();

  while (queue.length > 0) {
    const value = queue.shift()!;
    if (seen.has(value)) continue;
    seen.add(value);

    if (
      value.type === "tool" &&
      typeof value.tool === "string" &&
      typeof value.callID === "string"
    ) {
      return value as unknown as ToolPartLike;
    }

    for (const child of Object.values(value)) {
      if (child && typeof child === "object" && !Array.isArray(child)) {
        queue.push(child as Record<string, unknown>);
      }
    }
  }

  return null;
}
