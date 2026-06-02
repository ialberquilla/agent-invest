import { agentFetch, isAgentFetchError } from "@/lib/agent-client";
import { resolveUserIdentity } from "@/lib/proxy-auth";

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

  try {
    const body = (await request.json()) as Record<string, unknown>;
    const identity = await resolveUserIdentity(request, body);
    if (!identity.authenticated) {
      return Response.json({ message: "Login required" }, { status: 401 });
    }
    const response = await agentFetch(
      `/runs/${encodeURIComponent(id)}/vault`,
      { method: "POST", body },
    );
    return Response.json(await response.json());
  } catch (error) {
    return errorResponse(error);
  }
}
