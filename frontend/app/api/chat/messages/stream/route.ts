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
    const response = await agentFetch("/chat/messages/stream", {
      method: "POST",
      headers: { Accept: "text/event-stream" },
      body: {
        user_id: identity.userId,
        chat_session_id: body.chat_session_id,
        message: body.message,
      },
    });

    if (!response.body) {
      return Response.json(
        { message: "Upstream returned no body" },
        { status: 502 },
      );
    }

    return new Response(response.body, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
        "X-Accel-Buffering": "no",
      },
    });
  } catch (error) {
    return errorResponse(error);
  }
}
