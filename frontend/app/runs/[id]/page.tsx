import { notFound } from "next/navigation";

import { RunDetailClient } from "@/components/RunDetailClient";
import { agentFetch, isAgentFetchError } from "@/lib/agent-client";
import { Run } from "@/lib/types";

export const dynamic = "force-dynamic";

async function loadRun(id: string): Promise<Run> {
  try {
    const response = await agentFetch(`/runs/${encodeURIComponent(id)}`, {
      cache: "no-store",
    });
    return (await response.json()) as Run;
  } catch (error) {
    if (isAgentFetchError(error) && error.status === 404) {
      notFound();
    }

    throw error;
  }
}

export default async function RunPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const run = await loadRun(id);

  return <RunDetailClient run={run} />;
}
