import { agentFetch } from "@/lib/agent-client";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    await agentFetch("/health");
    return Response.json(
      { ok: true },
      { headers: { "cache-control": "no-store" } },
    );
  } catch {
    return Response.json(
      { ok: false },
      { status: 200, headers: { "cache-control": "no-store" } },
    );
  }
}
