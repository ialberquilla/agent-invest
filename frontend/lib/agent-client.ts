import "server-only";

type JsonBody = Record<string, unknown> | unknown[];

export type AgentFetchError = Error & { status: number };
type AgentFetchInit = Omit<RequestInit, "body"> & {
  body?: RequestInit["body"] | JsonBody | null;
};

function buildAgentUrl(path: string) {
  const agentUrl = process.env.AGENT_URL?.trim();
  if (!agentUrl) {
    throw new Error("AGENT_URL is not set");
  }

  return new URL(path, agentUrl);
}

function isJsonBody(body: AgentFetchInit["body"]): body is JsonBody {
  if (body === null || body === undefined) return false;
  if (Array.isArray(body)) return true;
  if (typeof body !== "object") return false;

  const prototype = Object.getPrototypeOf(body);
  return prototype === Object.prototype || prototype === null;
}

function withJsonBody(init: AgentFetchInit = {}) {
  if (init.body === undefined) {
    return init;
  }

  const headers = new Headers(init.headers);
  if (isJsonBody(init.body) && !headers.has("content-type")) {
    headers.set("content-type", "application/json");
  }

  return {
    ...init,
    body: isJsonBody(init.body) ? JSON.stringify(init.body) : init.body,
    headers,
  };
}

function readErrorMessage(body: string) {
  const text = body.trim();
  if (!text) return undefined;

  try {
    const parsed = JSON.parse(text) as { message?: unknown };
    if (typeof parsed.message === "string" && parsed.message.trim()) {
      return parsed.message.trim();
    }
  } catch {
    // Fall back to the plain-text body when the upstream response is not JSON.
  }

  return text;
}

function agentError(response: Response, body: string): AgentFetchError {
  const message =
    readErrorMessage(body) ??
    `Agent request failed with status ${response.status}`;
  return Object.assign(new Error(message), { status: response.status });
}

export function isAgentFetchError(error: unknown): error is AgentFetchError {
  return (
    error instanceof Error &&
    typeof (error as AgentFetchError).status === "number"
  );
}

export async function agentFetch(path: string, init?: AgentFetchInit) {
  const response = await fetch(buildAgentUrl(path), withJsonBody(init));

  if (response.ok) {
    return response;
  }

  throw agentError(response, await response.text());
}
