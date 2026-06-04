#!/usr/bin/env node

const PROTOCOL_VERSION = "2024-11-05";
const TOOL_NAME = "run_strategy_pipeline";

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
        serverInfo: { name: "agent-invest", version: "0.0.0" },
      };
    case "ping":
      return {};
    case "tools/list":
      return { tools: [toolDefinition()] };
    case "tools/call":
      return callTool(message.params);
    default:
      throw Object.assign(new Error(`Unknown MCP method: ${message.method}`), {
        code: -32601,
      });
  }
}

function toolDefinition() {
  return {
    name: TOOL_NAME,
    description:
      "Start an asynchronous Agent Invest strategy research workflow from an investment brief. Use only when the user asks to build, test, run, launch, or evaluate a strategy.",
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
      },
      required: ["brief", "chat_session_id"],
      additionalProperties: false,
    },
  };
}

async function callTool(params) {
  if (params?.name !== TOOL_NAME) {
    throw Object.assign(new Error(`Unknown tool: ${params?.name}`), {
      code: -32602,
    });
  }

  const args = params.arguments ?? {};
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

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify({ brief, chat_session_id: chatSessionId }),
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
  process.stderr.write(`[agent-invest-mcp] ${message}\n`);
}
