import { agentFetch } from "@/lib/agent-client";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const upstream = new URLSearchParams();
  for (const key of ["stage", "fixture_id", "limit"]) {
    const value = url.searchParams.get(key);
    if (value) upstream.set(key, value);
  }

  const query = upstream.toString();
  const response = await agentFetch(`/dev/evals${query ? `?${query}` : ""}`, {
    cache: "no-store",
  });

  return Response.json(await response.json());
}
