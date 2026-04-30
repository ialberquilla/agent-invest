import "server-only";

type AgentFetchError = Error & { status: number };

function buildAgentUrl(path: string) {
  const agentUrl = process.env.AGENT_URL?.trim();
  if (!agentUrl) {
    throw new Error("AGENT_URL is not set");
  }

  return new URL(path, agentUrl);
}

function withJsonContentType(init: RequestInit = {}) {
  if (init.body === undefined) {
    return init;
  }

  const headers = new Headers(init.headers);
  if (!headers.has("content-type")) {
    headers.set("content-type", "application/json");
  }

  return { ...init, headers };
}

function agentError(response: Response, body: string): AgentFetchError {
  const message =
    body.trim() || `Agent request failed with status ${response.status}`;
  return Object.assign(new Error(message), { status: response.status });
}

export async function agentFetch(path: string, init?: RequestInit) {
  const response = await fetch(buildAgentUrl(path), withJsonContentType(init));

  if (response.ok) {
    return response;
  }

  throw agentError(response, await response.text());
}
