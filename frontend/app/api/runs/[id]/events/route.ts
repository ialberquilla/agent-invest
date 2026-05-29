import { agentFetch, isAgentFetchError } from "@/lib/agent-client";

function errorResponse(error: unknown) {
  if (
    isAgentFetchError(error) &&
    (error.status === 400 || error.status === 404)
  ) {
    return Response.json({ message: error.message }, { status: error.status });
  }

  return Response.json({ message: "Internal Server Error" }, { status: 500 });
}

export async function GET(
  request: Request,
  context: { params: Promise<{ id: string }> },
) {
  const { id } = await context.params;
  const requestUrl = new URL(request.url);
  const upstreamPath = new URLSearchParams();

  for (const key of ["stage", "round"]) {
    const value = requestUrl.searchParams.get(key);
    if (value !== null) upstreamPath.set(key, value);
  }

  const query = upstreamPath.toString();

  try {
    const response = await agentFetch(
      `/runs/${encodeURIComponent(id)}/events${query ? `?${query}` : ""}`,
    );
    return Response.json(await response.json());
  } catch (error) {
    return errorResponse(error);
  }
}
