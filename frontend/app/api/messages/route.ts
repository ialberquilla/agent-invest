import { agentFetch, isAgentFetchError } from "@/lib/agent-client";
import { USER_ID } from "@/lib/constants";

type MessageRequestBody = {
  strategy_id?: unknown;
  text?: unknown;
};

function errorResponse(error: unknown) {
  if (isAgentFetchError(error) && error.status >= 400 && error.status < 500) {
    return Response.json({ message: error.message }, { status: error.status });
  }

  return Response.json({ message: "Internal Server Error" }, { status: 500 });
}

export async function POST(request: Request) {
  let body: MessageRequestBody = {};

  try {
    const json = (await request.json()) as unknown;
    if (json && typeof json === "object" && !Array.isArray(json)) {
      body = json as MessageRequestBody;
    }
  } catch {
    return Response.json(
      { message: "Request body must be valid JSON" },
      { status: 400 },
    );
  }

  try {
    const response = await agentFetch("/messages", {
      method: "POST",
      body: {
        user_id: USER_ID,
        strategy_id: body.strategy_id,
        text: body.text,
      },
    });

    return Response.json(await response.json());
  } catch (error) {
    return errorResponse(error);
  }
}
