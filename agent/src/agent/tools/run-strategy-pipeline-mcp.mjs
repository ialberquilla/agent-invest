#!/usr/bin/env node

const PROTOCOL_VERSION = "2024-11-05";
const RUN_STRATEGY_PIPELINE_TOOL_NAME = "run_strategy_pipeline";
const SCREEN_MARKETS_TOOL_NAME = "screen_markets";
const RUN_RESEARCH_CODE_TOOL_NAME = "run_research_code";

let framing = "line";
let buffer = Buffer.alloc(0);

process.stdin.on("data", (chunk) => {
  buffer = Buffer.concat([buffer, chunk]);
  processBuffer();
});

process.stdin.on("end", () => process.exit(0));

function processBuffer() {
  while (buffer.length > 0) {
    const headerEnd = buffer.indexOf("\r\n\r\n");
    if (headerEnd !== -1) {
      framing = "header";
      const header = buffer.slice(0, headerEnd).toString("utf8");
      const match = /content-length:\s*(\d+)/i.exec(header);
      if (!match) throw new Error("MCP message missing Content-Length");
      const length = Number(match[1]);
      const start = headerEnd + 4;
      const end = start + length;
      if (buffer.length < end) return;
      const raw = buffer.slice(start, end).toString("utf8");
      buffer = buffer.slice(end);
      void handleRawMessage(raw);
      continue;
    }

    const lineEnd = buffer.indexOf("\n");
    if (lineEnd === -1) return;
    framing = "line";
    const raw = buffer.slice(0, lineEnd).toString("utf8").trim();
    buffer = buffer.slice(lineEnd + 1);
    if (raw) void handleRawMessage(raw);
  }
}

async function handleRawMessage(raw) {
  let message;
  try {
    message = JSON.parse(raw);
  } catch (error) {
    log(`invalid JSON-RPC message: ${errorMessage(error)}`);
    return;
  }

  if (message.id === undefined || message.id === null) {
    return;
  }

  try {
    const result = await handleRequest(message);
    send({ jsonrpc: "2.0", id: message.id, result });
  } catch (error) {
    send({
      jsonrpc: "2.0",
      id: message.id,
      error: {
        code: error.code ?? -32000,
        message: errorMessage(error),
      },
    });
  }
}

async function handleRequest(message) {
  switch (message.method) {
    case "initialize":
      return {
        protocolVersion:
          message.params?.protocolVersion ?? PROTOCOL_VERSION,
        capabilities: { tools: {} },
        serverInfo: { name: "pond3r-portfolio", version: "0.0.0" },
      };
    case "ping":
      return {};
    case "tools/list":
      return { tools: [runStrategyPipelineToolDefinition(), screenMarketsToolDefinition(), runResearchCodeToolDefinition()] };
    case "tools/call":
      return callTool(message.params);
    default:
      throw Object.assign(new Error(`Unknown MCP method: ${message.method}`), {
        code: -32601,
      });
  }
}

function runStrategyPipelineToolDefinition() {
  return {
    name: RUN_STRATEGY_PIPELINE_TOOL_NAME,
    description:
      "Start an asynchronous Agent Invest strategy research workflow from an investment brief. Use only when the user asks to build, test, run, launch, or evaluate a strategy. To iterate on a prior run (e.g. 'try fewer assets', 'lower the max drawdown'), pass based_on_run_id and an overrides object instead of re-briefing from scratch.",
    inputSchema: {
      type: "object",
      properties: {
        brief: {
          type: "string",
          description: "The user's investment strategy brief.",
        },
        chat_session_id: {
          type: "string",
          description:
            "The current Agent Invest chat_session_id from the system prompt.",
        },
        based_on_run_id: {
          type: "string",
          description:
            "Optional. The run_id of a prior strategy run this one iterates on. Records provenance for the rerun.",
        },
        overrides: {
          type: "object",
          description:
            "Optional deterministic reshaping of the interpreted thesis for a rerun. Only set the fields the user asked to change.",
          properties: {
            asset_count_min: { type: "integer", minimum: 1 },
            asset_count_max: { type: "integer", minimum: 1 },
            max_weight_per_asset: { type: "number", minimum: 0, maximum: 1 },
            max_cash_weight: { type: "number", minimum: 0, maximum: 1 },
            max_drawdown: { type: "number", minimum: 0, maximum: 1 },
            horizon_days: { type: "integer", minimum: 30 },
            rebalance_frequency: {
              type: "string",
              enum: ["daily", "weekly", "monthly", "quarterly"],
            },
            top_n: { type: "integer", minimum: 1 },
            top_skip: { type: "integer", minimum: 0 },
            exclude_stablecoins: { type: "boolean" },
            exclude_wrapped: { type: "boolean" },
            hand_picked_coin_ids: {
              type: "array",
              items: { type: "string" },
            },
            strategy_mode: {
              type: "string",
              enum: [
                "single_asset",
                "pair_trade",
                "hedge_overlay",
                "basket_allocation",
                "momentum_rotation",
                "long_short_portfolio",
              ],
            },
            target_coin_id: { type: "string" },
          },
          additionalProperties: false,
        },
        data_as_of: {
          type: "string",
          description:
            "Optional ISO date pinning the data snapshot for reproducibility.",
        },
      },
      required: ["brief", "chat_session_id"],
      additionalProperties: false,
    },
  };
}

function screenMarketsToolDefinition() {
  return {
    name: SCREEN_MARKETS_TOOL_NAME,
    description:
      "Return a read-only structured crypto market screener backed by rank_universe and GMX V2 market resolution. Use for top momentum, Sharpe, or low-volatility ticker screens; do not start a strategy pipeline.",
    inputSchema: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description: "The user's screener request in plain language.",
        },
        factor: {
          type: "string",
          enum: ["momentum", "risk_adjusted", "low_volatility"],
          description: "The ranking family to use.",
        },
        limit: {
          type: "integer",
          minimum: 1,
          maximum: 25,
          description: "Maximum rows to return.",
        },
        gmxOnly: {
          type: "boolean",
          description:
            "When true, return only rows resolved to executable Arbitrum GMX V2 markets. Defaults to true.",
        },
      },
      required: ["query"],
      additionalProperties: false,
    },
  };
}

function runResearchCodeToolDefinition() {
  return {
    name: RUN_RESEARCH_CODE_TOOL_NAME,
    description:
      "Run short Python research code against read-only market-data views. Use for bespoke historical/statistical analysis, event studies, regime splits, and return distributions that are not market screeners or full strategy workflows.",
    inputSchema: {
      type: "object",
      properties: {
        code: {
          type: "string",
          description:
            "Python code to execute. Must use query(sql) for data access and put the final JSON-serializable answer in result.",
        },
        purpose: {
          type: "string",
          description: "One-line description of the research question.",
        },
      },
      required: ["code", "purpose"],
      additionalProperties: false,
    },
  };
}

async function callTool(params) {
  if (params?.name === RUN_STRATEGY_PIPELINE_TOOL_NAME) {
    return callRunStrategyPipeline(params.arguments ?? {});
  }
  if (params?.name === SCREEN_MARKETS_TOOL_NAME) {
    return callScreenMarkets(params.arguments ?? {});
  }
  if (params?.name === RUN_RESEARCH_CODE_TOOL_NAME) {
    return callRunResearchCode(params.arguments ?? {});
  }

  throw Object.assign(new Error(`Unknown tool: ${params?.name}`), {
    code: -32602,
  });
}

async function callRunResearchCode(args) {
  const code = typeof args.code === "string" ? args.code.trim() : "";
  const purpose = typeof args.purpose === "string" ? args.purpose.trim() : "";
  if (!code) throw Object.assign(new Error("code is required"), { code: -32602 });
  if (!purpose) throw Object.assign(new Error("purpose is required"), { code: -32602 });

  const url = new URL("/internal/tools/run-research-code", agentToolUrl());
  const headers = { "content-type": "application/json" };
  if (process.env.AGENT_API_KEY) {
    headers["x-api-key"] = process.env.AGENT_API_KEY;
  }
  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify({ code, purpose }),
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(text || `Agent API returned ${response.status}`);
  }
  const payload = text ? JSON.parse(text) : {};
  return { content: [{ type: "text", text: JSON.stringify(payload) }] };
}

async function callRunStrategyPipeline(args) {
  const brief = typeof args.brief === "string" ? args.brief.trim() : "";
  const chatSessionId =
    typeof args.chat_session_id === "string" ? args.chat_session_id.trim() : "";

  if (!brief) {
    throw Object.assign(new Error("brief is required"), { code: -32602 });
  }
  if (!chatSessionId) {
    throw Object.assign(new Error("chat_session_id is required"), {
      code: -32602,
    });
  }

  const url = new URL("/internal/tools/run-strategy-pipeline", agentToolUrl());
  const headers = { "content-type": "application/json" };
  if (process.env.AGENT_API_KEY) {
    headers["x-api-key"] = process.env.AGENT_API_KEY;
  }

  const requestBody = { brief, chat_session_id: chatSessionId };
  if (typeof args.based_on_run_id === "string" && args.based_on_run_id.trim()) {
    requestBody.based_on_run_id = args.based_on_run_id.trim();
  }
  if (args.overrides !== undefined && args.overrides !== null) {
    requestBody.overrides = args.overrides;
  }
  if (typeof args.data_as_of === "string" && args.data_as_of.trim()) {
    requestBody.data_as_of = args.data_as_of.trim();
  }

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(requestBody),
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(text || `Agent API returned ${response.status}`);
  }

  const payload = text ? JSON.parse(text) : {};
  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(payload),
      },
    ],
  };
}

async function callScreenMarkets(args) {
  const query = typeof args.query === "string" ? args.query.trim() : "";
  if (!query) {
    throw Object.assign(new Error("query is required"), { code: -32602 });
  }
  const body = { query };
  if (args.factor !== undefined) body.factor = args.factor;
  if (args.limit !== undefined) body.limit = args.limit;
  if (args.gmxOnly !== undefined) body.gmxOnly = args.gmxOnly;

  const url = new URL("/internal/tools/screen-markets", agentToolUrl());
  const headers = { "content-type": "application/json" };
  if (process.env.AGENT_API_KEY) {
    headers["x-api-key"] = process.env.AGENT_API_KEY;
  }

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(text || `Agent API returned ${response.status}`);
  }

  const payload = text ? JSON.parse(text) : {};
  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(payload),
      },
    ],
  };
}

function agentToolUrl() {
  const value = process.env.AGENT_TOOL_URL?.trim();
  if (!value) throw new Error("AGENT_TOOL_URL is not configured");
  return value;
}

function send(message) {
  const raw = JSON.stringify(message);
  if (framing === "header") {
    process.stdout.write(
      `Content-Length: ${Buffer.byteLength(raw, "utf8")}\r\n\r\n${raw}`,
    );
    return;
  }
  process.stdout.write(`${raw}\n`);
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

function log(message) {
  process.stderr.write(`[pond3r-portfolio-mcp] ${message}\n`);
}
