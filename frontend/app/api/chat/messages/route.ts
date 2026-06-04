import { agentFetch, isAgentFetchError } from "@/lib/agent-client";
import { resolveUserIdentity } from "@/lib/proxy-auth";

type ChatMessageRequestBody = {
  chat_session_id?: unknown;
  user_id?: unknown;
  message?: unknown;
};

function errorResponse(error: unknown) {
  if (isAgentFetchError(error) && error.status >= 400 && error.status < 500) {
    return Response.json({ message: error.message }, { status: error.status });
  }
  return Response.json({ message: "Internal Server Error" }, { status: 500 });
}

export async function POST(request: Request) {
  let body: ChatMessageRequestBody = {};
  try {
    const json = (await request.json()) as unknown;
    if (json && typeof json === "object" && !Array.isArray(json)) {
      body = json as ChatMessageRequestBody;
    }
  } catch {
    return Response.json(
      { message: "Request body must be valid JSON" },
      { status: 400 },
    );
  }

  try {
    const identity = await resolveUserIdentity(request, body);
    const response = await agentFetch("/chat/messages", {
      method: "POST",
      body: {
        user_id: identity.userId,
        chat_session_id: body.chat_session_id,
        message: body.message,
      },
    });

    return Response.json(await response.json());
  } catch (error) {
    return errorResponse(error);
  }
}
