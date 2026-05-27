import { notFound } from "next/navigation";

import {
  DevEvalsInspector,
  type EvalRunSummary,
} from "@/components/DevEvalsInspector";
import { agentFetch } from "@/lib/agent-client";

export const dynamic = "force-dynamic";

type SearchParams = {
  dev?: string;
  stage?: string;
  fixture_id?: string;
};

async function loadEvalRuns(searchParams: SearchParams) {
  const upstream = new URLSearchParams();
  if (searchParams.stage) upstream.set("stage", searchParams.stage);
  if (searchParams.fixture_id) upstream.set("fixture_id", searchParams.fixture_id);
  const query = upstream.toString();
  const response = await agentFetch(`/dev/evals${query ? `?${query}` : ""}`, {
    cache: "no-store",
  });

  return (await response.json()) as EvalRunSummary[];
}

export default async function DevEvalsPage({
  searchParams,
}: {
  searchParams: Promise<SearchParams>;
}) {
  const resolvedSearchParams = await searchParams;
  if (resolvedSearchParams.dev !== "1") notFound();

  const runs = await loadEvalRuns(resolvedSearchParams);

  return (
    <DevEvalsInspector
      initialRuns={runs}
      initialStage={resolvedSearchParams.stage}
      initialFixtureId={resolvedSearchParams.fixture_id}
    />
  );
}
