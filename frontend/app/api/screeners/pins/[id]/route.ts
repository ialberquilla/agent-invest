import { agentFetch, isAgentFetchError } from "@/lib/agent-client";

function errorResponse(error: unknown) {
  if (isAgentFetchError(error) && error.status >= 400 && error.status < 500) {
    return Response.json({ message: error.message }, { status: error.status });
  }
  return Response.json({ message: "Internal Server Error" }, { status: 500 });
}

export async function DELETE(
  request: Request,
  context: { params: Promise<{ id: string }> },
) {
  const { id } = await context.params;
  const url = new URL(request.url);
  const userId = url.searchParams.get("user_id")?.trim();
  if (!userId) {
    return Response.json({ message: "Query must include user_id" }, { status: 400 });
  }

  try {
    const response = await agentFetch(
      `/screeners/pins/${encodeURIComponent(id)}?user_id=${encodeURIComponent(userId)}`,
      { method: "DELETE" },
    );
    return Response.json(await response.json());
  } catch (error) {
    return errorResponse(error);
  }
}
