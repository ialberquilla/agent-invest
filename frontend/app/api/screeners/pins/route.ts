import { agentFetch, isAgentFetchError } from "@/lib/agent-client";

function errorResponse(error: unknown) {
  if (isAgentFetchError(error) && error.status >= 400 && error.status < 500) {
    return Response.json({ message: error.message }, { status: error.status });
  }
  return Response.json({ message: "Internal Server Error" }, { status: 500 });
}

export async function GET(request: Request) {
  const url = new URL(request.url);
  const userId = url.searchParams.get("user_id")?.trim();
  if (!userId) {
    return Response.json({ message: "Query must include user_id" }, { status: 400 });
  }

  try {
    const response = await agentFetch(
      `/screeners/pins?user_id=${encodeURIComponent(userId)}`,
      { method: "GET" },
    );
    return Response.json(await response.json());
  } catch (error) {
    return errorResponse(error);
  }
}

export async function POST(request: Request) {
  let body: Record<string, unknown> = {};
  try {
    const json = (await request.json()) as unknown;
    if (json && typeof json === "object" && !Array.isArray(json)) {
      body = json as Record<string, unknown>;
    }
  } catch {
    return Response.json(
      { message: "Request body must be valid JSON" },
      { status: 400 },
    );
  }

  try {
    const response = await agentFetch("/screeners/pins", {
      method: "POST",
      body,
    });
    return Response.json(await response.json());
  } catch (error) {
    return errorResponse(error);
  }
}
