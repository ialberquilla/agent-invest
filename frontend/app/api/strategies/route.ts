import { agentFetch, isAgentFetchError } from "@/lib/agent-client";
import { USER_ID } from "@/lib/constants";

function errorResponse(error: unknown) {
  if (isAgentFetchError(error) && error.status >= 400 && error.status < 500) {
    return Response.json({ message: error.message }, { status: error.status });
  }

  return Response.json({ message: "Internal Server Error" }, { status: 500 });
}

export async function POST() {
  try {
    const response = await agentFetch("/strategies", {
      method: "POST",
      body: { user_id: USER_ID },
    });

    return Response.json((await response.json()) as { strategy_id: string });
  } catch (error) {
    return errorResponse(error);
  }
}
