import { agentFetch, isAgentFetchError } from "@/lib/agent-client";

function errorResponse(error: unknown) {
  if (isAgentFetchError(error) && error.status >= 400 && error.status < 500) {
    return Response.json({ message: error.message }, { status: error.status });
  }
  return Response.json({ message: "Internal Server Error" }, { status: 500 });
}

export async function POST(
  request: Request,
  context: { params: Promise<{ id: string }> },
) {
  const { id } = await context.params;
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
    const response = await agentFetch(
      `/screeners/pins/${encodeURIComponent(id)}/refresh`,
      { method: "POST", body },
    );
    return Response.json(await response.json());
  } catch (error) {
    return errorResponse(error);
  }
}
