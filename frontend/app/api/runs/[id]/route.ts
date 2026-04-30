import { agentFetch, isAgentFetchError } from "@/lib/agent-client";

function errorResponse(error: unknown) {
  if (isAgentFetchError(error) && error.status === 404) {
    return Response.json({ message: error.message }, { status: 404 });
  }

  return Response.json({ message: "Internal Server Error" }, { status: 500 });
}

export async function GET(
  _request: Request,
  context: { params: Promise<{ id: string }> },
) {
  const { id } = await context.params;

  try {
    const response = await agentFetch(`/runs/${encodeURIComponent(id)}`);
    return Response.json(await response.json());
  } catch (error) {
    return errorResponse(error);
  }
}
