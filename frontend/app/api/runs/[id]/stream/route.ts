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
    const upstream = await agentFetch(
      `/runs/${encodeURIComponent(id)}/stream`,
      { headers: { Accept: "text/event-stream" } },
    );

    if (!upstream.body) {
      return Response.json(
        { message: "Upstream returned no body" },
        { status: 502 },
      );
    }

    return new Response(upstream.body, {
      headers: {
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
        "Content-Type": "text/event-stream",
      },
    });
  } catch (error) {
    return errorResponse(error);
  }
}
