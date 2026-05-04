import { agentFetch, isAgentFetchError } from "@/lib/agent-client";

const FORWARDED_HEADERS = ["content-type", "content-length", "cache-control"];

function errorResponse(error: unknown) {
  if (isAgentFetchError(error)) {
    return Response.json({ message: error.message }, { status: error.status });
  }
  return Response.json({ message: "Internal Server Error" }, { status: 500 });
}

export async function GET(
  _request: Request,
  context: { params: Promise<{ path: string[] }> },
) {
  const { path } = await context.params;
  if (!path || path.length === 0) {
    return Response.json({ message: "Artifact path required" }, { status: 400 });
  }

  const safe = path
    .map((segment) => encodeURIComponent(segment))
    .join("/");

  try {
    const upstream = await agentFetch(`/artifacts/${safe}`, { method: "GET" });
    if (!upstream.body) {
      return Response.json(
        { message: "Upstream returned no body" },
        { status: 502 },
      );
    }

    const headers = new Headers();
    for (const name of FORWARDED_HEADERS) {
      const value = upstream.headers.get(name);
      if (value) headers.set(name, value);
    }

    return new Response(upstream.body, { headers });
  } catch (error) {
    return errorResponse(error);
  }
}
