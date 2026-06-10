import { agentFetch, isAgentFetchError } from "@/lib/agent-client";

function errorResponse(error: unknown) {
  if (isAgentFetchError(error) && error.status >= 400 && error.status < 500) {
    return Response.json({ message: error.message }, { status: error.status });
  }
  return Response.json({ message: "Internal Server Error" }, { status: 500 });
}

export async function GET() {
  try {
    const response = await agentFetch("/markets/gmx", { method: "GET" });
    return Response.json(await response.json());
  } catch (error) {
    return errorResponse(error);
  }
}
