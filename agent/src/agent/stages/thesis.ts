import {
  createStageRunner,
  type StageDefinition,
  type StageRunner,
} from "./base";
import {
  buildAllocationWizardPrompt,
  type AllocationWizardParams,
} from "../prompt";
import type { RecordThesisInput } from "../../tools/record-investment-thesis";

export type ThesisJson = RecordThesisInput;

export type ThesisStageOutput = {
  thesis_id: string;
  thesis: ThesisJson;
};

export type ThesisStageInput = {
  run_id: string;
  wizard?: AllocationWizardParams;
  request?: string;
};

type ThesisRunnerInput = {
  run_id: string;
  request: string;
};

export const thesisStage: StageDefinition = {
  name: "thesis",
  allowedTools: ["record_investment_thesis"],
  terminal: "required",
  promptPath: "thesis.md",
};

export function formatThesisStageInput(
  input: ThesisStageInput,
): ThesisRunnerInput {
  if (input.wizard) {
    return {
      run_id: input.run_id,
      request: buildAllocationWizardPrompt(input.wizard),
    };
  }

  return {
    run_id: input.run_id,
    request: input.request ?? "",
  };
}

export function createThesisStageRunner(
  ...args: Parameters<
    typeof createStageRunner<ThesisRunnerInput, ThesisStageOutput>
  >
): StageRunner<ThesisStageInput, ThesisStageOutput> {
  const runner = createStageRunner<ThesisRunnerInput, ThesisStageOutput>(
    ...args,
  );

  return {
    run(input, runId, round) {
      return runner.run(formatThesisStageInput(input), runId, round);
    },
  };
}

export const thesisRunner = createThesisStageRunner(thesisStage);
