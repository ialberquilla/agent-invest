import {
  createStageRunner,
  type StageDefinition,
  type StageRunner,
} from "./base";
import type { Candidate } from "../../tools/run-candidate-batch";
import type { ThesisJson } from "./thesis";

export type DesignerStageInput = {
  run_id: string;
  round: 1 | 2 | 3;
  thesis: ThesisJson;
  prior_batch_id?: string | null;
  refinement_reasons?: Array<Record<string, unknown>>;
};

export type DesignerStageOutput = {
  batch_id: string;
  candidates: Candidate[];
  kpis: Record<string, unknown>;
};

export const designerStage: StageDefinition = {
  name: "designer",
  allowedTools: [
    "list_templates",
    "list_registry",
    "rank_universe",
    "analyze_recovery",
    "recommend_backtest_window",
    "run_candidate_batch",
  ],
  terminal: "required",
  promptPath: "designer.md",
};

export function createDesignerStageRunner(
  ...args: Parameters<
    typeof createStageRunner<DesignerStageInput, DesignerStageOutput>
  >
): StageRunner<DesignerStageInput, DesignerStageOutput> {
  return createStageRunner<DesignerStageInput, DesignerStageOutput>(...args);
}

export const designerRunner = createDesignerStageRunner(designerStage);
