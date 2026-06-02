import { agentFetch, isAgentFetchError } from "@/lib/agent-client";
import { resolveUserIdentity } from "@/lib/proxy-auth";

function errorResponse(error: unknown) {
  if (isAgentFetchError(error) && error.status >= 400 && error.status < 500) {
    return Response.json({ message: error.message }, { status: error.status });
  }

  return Response.json({ message: "Internal Server Error" }, { status: 500 });
}

export async function POST(request: Request) {
  const body = (await request.json().catch(() => ({}))) as Record<
    string,
    unknown
  >;
  const identity = await resolveUserIdentity(request, body);
  if (!identity.authenticated) {
    return Response.json({ message: "Login required" }, { status: 401 });
  }

  try {
    const response = await agentFetch(
      `/users/${encodeURIComponent(identity.userId)}/claim`,
      { method: "POST", body: { anonymous_user_id: body.user_id } },
    );
    return Response.json(await response.json());
  } catch (error) {
    return errorResponse(error);
  }
}
