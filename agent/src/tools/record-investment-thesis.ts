import { createInvestmentThesis } from "../db/repositories/investment-theses";

const OBJECTIVES = [
  "high_growth",
  "balanced",
  "preserve_capital",
  "income",
] as const;

export type ThesisObjective = (typeof OBJECTIVES)[number];

export type ThesisPrimaryFactor = {
  factor: string;
  direction: "high" | "low";
};

export type ThesisConstraints = {
  max_assets?: number;
  min_assets?: number;
  max_allocation_pct?: number;
  min_market_cap_usd?: number;
  excluded_assets?: string[];
  required_assets?: string[];
  [key: string]: unknown;
};

export type RecordThesisInput = {
  run_id: string;
  objective: ThesisObjective;
  primary_factors: ThesisPrimaryFactor[];
  constraints?: ThesisConstraints;
  horizon_days: number;
  interpretation_notes: string;
};

export type RecordThesisOutput = {
  thesis_id: string;
};

type Db = Parameters<typeof createInvestmentThesis>[1];

export async function recordInvestmentThesis(
  input: RecordThesisInput,
  db?: Db,
): Promise<RecordThesisOutput> {
  validateInput(input);

  const thesis = await createInvestmentThesis(
    {
      objective: input.objective,
      payload: input as unknown as Record<string, unknown>,
      runId: input.run_id,
    },
    db,
  );

  return { thesis_id: thesis.thesisId };
}

function validateInput(input: RecordThesisInput) {
  if (typeof input.run_id !== "string" || input.run_id.trim().length === 0) {
    throw new Error("run_id is required");
  }
  if (!(OBJECTIVES as readonly string[]).includes(input.objective)) {
    throw new Error(
      "objective must be one of high_growth, balanced, preserve_capital, income",
    );
  }
  if (
    !Array.isArray(input.primary_factors) ||
    input.primary_factors.length === 0
  ) {
    throw new Error("primary_factors must be non-empty");
  }
  for (const [index, primaryFactor] of input.primary_factors.entries()) {
    if (!isRecord(primaryFactor)) {
      throw new Error(`primary_factors[${index}] must be an object`);
    }
    if (
      typeof primaryFactor.factor !== "string" ||
      primaryFactor.factor.trim().length === 0
    ) {
      throw new Error(
        `primary_factors[${index}].factor must be a non-empty string`,
      );
    }
    if (!/^[a-z][a-z0-9_:.\-]*$/i.test(primaryFactor.factor)) {
      throw new Error(
        `primary_factors[${index}].factor must be a registry factor id`,
      );
    }
    if (
      primaryFactor.direction !== "high" &&
      primaryFactor.direction !== "low"
    ) {
      throw new Error(
        `primary_factors[${index}].direction must be high or low`,
      );
    }
  }
  if (input.constraints !== undefined && !isRecord(input.constraints)) {
    throw new Error("constraints must be an object");
  }
  if (!Number.isInteger(input.horizon_days) || input.horizon_days < 1) {
    throw new Error("horizon_days must be a positive integer");
  }
  if (
    typeof input.interpretation_notes !== "string" ||
    input.interpretation_notes.trim().length === 0
  ) {
    throw new Error("interpretation_notes must be a non-empty string");
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
