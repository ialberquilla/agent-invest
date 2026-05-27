import { agentFetch, isAgentFetchError } from "@/lib/agent-client";

export async function GET(
  _request: Request,
  context: { params: Promise<{ id: string }> },
) {
  const { id } = await context.params;

  try {
    const response = await agentFetch(`/dev/evals/${encodeURIComponent(id)}`, {
      cache: "no-store",
    });
    return Response.json(await response.json());
  } catch (error) {
    if (isAgentFetchError(error) && error.status === 404) {
      return Response.json({ message: error.message }, { status: 404 });
    }
    return Response.json({ message: "Internal Server Error" }, { status: 500 });
  }
}
