import { agentFetch, isAgentFetchError } from "@/lib/agent-client";
import { resolveUserIdentity } from "@/lib/proxy-auth";

function errorResponse(error: unknown) {
  if (isAgentFetchError(error) && error.status >= 400 && error.status < 500) {
    return Response.json({ message: error.message }, { status: error.status });
  }

  return Response.json({ message: "Internal Server Error" }, { status: 500 });
}

export async function POST(request: Request) {
  try {
    const body = (await request.json().catch(() => ({}))) as Record<
      string,
      unknown
    >;
    const identity = await resolveUserIdentity(request, body);
    const response = await agentFetch("/strategies", {
      method: "POST",
      body: { user_id: identity.userId },
    });

    return Response.json((await response.json()) as { strategy_id: string });
  } catch (error) {
    return errorResponse(error);
  }
}
