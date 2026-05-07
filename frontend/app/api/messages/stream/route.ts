import { agentFetch, isAgentFetchError } from "@/lib/agent-client";
import { USER_ID } from "@/lib/constants";

type StreamRequestBody = {
  strategy_id?: unknown;
  text?: unknown;
  wizard_params?: unknown;
};

function errorResponse(error: unknown) {
  if (isAgentFetchError(error) && error.status >= 400 && error.status < 500) {
    return Response.json({ message: error.message }, { status: error.status });
  }
  return Response.json({ message: "Internal Server Error" }, { status: 500 });
}

export async function POST(request: Request) {
  let body: StreamRequestBody = {};
  try {
    const json = (await request.json()) as unknown;
    if (json && typeof json === "object" && !Array.isArray(json)) {
      body = json as StreamRequestBody;
    }
  } catch {
    return Response.json(
      { message: "Request body must be valid JSON" },
      { status: 400 },
    );
  }

  try {
    const upstream = await agentFetch("/messages/stream", {
      method: "POST",
      body: {
        user_id: USER_ID,
        strategy_id: body.strategy_id,
        text: body.text,
        wizard_params: body.wizard_params,
      },
    });

    if (!upstream.body) {
      return Response.json(
        { message: "Upstream returned no body" },
        { status: 502 },
      );
    }

    return new Response(upstream.body, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        Connection: "keep-alive",
      },
    });
  } catch (error) {
    return errorResponse(error);
  }
}
