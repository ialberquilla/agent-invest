import {
  adjudicatorRunner,
  type AdjudicatorStageOutput,
  type RefinementReason,
} from "./stages/adjudicator";
import { designerRunner, type DesignerStageOutput } from "./stages/designer";
import {
  reporterRunner,
  type ReporterRoundHistory,
  type ReporterStageOutput,
} from "./stages/reporter";
import {
  thesisRunner,
  type ThesisJson,
  type ThesisStageInput,
} from "./stages/thesis";
import type { StageRunner } from "./stages/base";
import {
  deterministicAdjudicatorRunner,
  deterministicDesignerRunner,
  deterministicReporterRunner,
} from "./stages/deterministic";

export type PipelineBrief = ThesisStageInput["wizard"] | string;

export type PipelineResult = ReporterStageOutput;

export type PipelineRunners = {
  thesis: StageRunner<
    ThesisStageInput,
    { thesis_id: string; thesis: ThesisJson }
  >;
  designer: StageRunner<
    {
      run_id: string;
      round: PipelineRound;
      thesis: ThesisJson;
      prior_batch_id?: string | null;
      refinement_reasons?: RefinementReason[];
    },
    DesignerStageOutput
  >;
  adjudicator: StageRunner<
    {
      run_id: string;
      round: PipelineRound;
      thesis: ThesisJson;
      batch_id: string;
      candidates: DesignerStageOutput["candidates"];
      kpis?: Record<string, unknown>;
    },
    AdjudicatorStageOutput
  >;
  reporter: StageRunner<
    {
      run_id: string;
      thesis: ThesisJson;
      candidate_batch_id: string;
      winner_candidate_id: string | null;
      round_history: ReporterRoundHistory[];
    },
    ReporterStageOutput
  >;
};

type PipelineRound = 1 | 2 | 3;

const MAX_ROUNDS = 3;

const defaultRunners: PipelineRunners = {
  thesis: thesisRunner,
  designer: deterministicDesignerRunner,
  adjudicator: deterministicAdjudicatorRunner,
  reporter: deterministicReporterRunner,
};

export async function runPipeline(
  runId: string,
  brief: PipelineBrief,
  runners: PipelineRunners = defaultRunners,
): Promise<PipelineResult> {
  const thesisInput = buildThesisInput(runId, brief);
  const history: ReporterRoundHistory[] = [];

  let thesis: ThesisJson | null = null;
  let lastBatch: DesignerStageOutput | null = null;

  try {
    thesis = (await runners.thesis.run(thesisInput, runId, 1)).thesis;
  } catch (error) {
    return runFailureReporter(
      runId,
      fallbackThesis(runId, error),
      null,
      history,
      runners,
    );
  }

  let refinementReasons: RefinementReason[] = [];
  for (let round = 1 as PipelineRound; round <= MAX_ROUNDS; round += 1) {
    try {
      const designerInput = {
        run_id: runId,
        round,
        thesis,
        prior_batch_id: lastBatch?.batch_id ?? null,
        refinement_reasons: refinementReasons,
      };
      const batch = await runners.designer.run(designerInput, runId, round);
      lastBatch = batch;

      const adjudication = await runners.adjudicator.run(
        {
          run_id: runId,
          round,
          thesis,
          batch_id: batch.batch_id,
          candidates: batch.candidates,
          kpis: batch.kpis,
        },
        runId,
        round,
      );

      history.push(roundHistory(round, batch.batch_id, adjudication));

      if (adjudication.kind === "winner") {
        return runners.reporter.run(
          {
            run_id: runId,
            thesis,
            candidate_batch_id: batch.batch_id,
            winner_candidate_id: adjudication.candidate_id,
            round_history: history,
          },
          runId,
          1,
        );
      }

      refinementReasons = adjudication.reasons;
    } catch {
      return runFailureReporter(runId, thesis, lastBatch, history, runners);
    }
  }

  return runFailureReporter(runId, thesis, lastBatch, history, runners);
}

function buildThesisInput(
  runId: string,
  brief: PipelineBrief,
): ThesisStageInput {
  if (typeof brief === "string") return { run_id: runId, request: brief };
  return { run_id: runId, wizard: brief };
}

function roundHistory(
  round: PipelineRound,
  candidateBatchId: string,
  adjudication: AdjudicatorStageOutput,
): ReporterRoundHistory {
  return {
    round,
    candidate_batch_id: candidateBatchId,
    adjudication,
    refinement_reasons:
      adjudication.kind === "refine" ? adjudication.reasons : undefined,
  };
}

function runFailureReporter(
  runId: string,
  thesis: ThesisJson,
  lastBatch: DesignerStageOutput | null,
  history: ReporterRoundHistory[],
  runners: PipelineRunners,
) {
  return runners.reporter.run(
    {
      run_id: runId,
      thesis,
      candidate_batch_id: lastBatch?.batch_id ?? "no_viable_strategy",
      winner_candidate_id: null,
      round_history: history,
    },
    runId,
    1,
  );
}

function fallbackThesis(runId: string, error: unknown): ThesisJson {
  return {
    run_id: runId,
    objective: "balanced",
    primary_factors: [{ factor: "no_viable_strategy", direction: "high" }],
    constraints: {},
    horizon_days: 1,
    interpretation_notes: `Thesis stage failed before producing structured output: ${errorMessage(error)}`,
  };
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}
