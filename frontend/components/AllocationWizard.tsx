"use client";

import { useEffect, useEffectEvent, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import type { AllocationWizardState } from "@/lib/wizard-prompt";
import { trackEvent } from "@/lib/analytics";

type Option<T extends string = string> = {
  value: T;
  label: string;
  description?: string;
};

const steps = [
  {
    title: "Universe",
    description: "Choose the investable assets and exclusions.",
  },
  {
    title: "Market Filters",
    description: "Set market cap and concentration guardrails.",
  },
  {
    title: "Risk Tolerance",
    description: "Set drawdown tolerance and risk posture.",
  },
  {
    title: "Objective and Time Horizon",
    description: "Define the time horizon and rebalance cadence.",
  },
  {
    title: "Portfolio Constraints",
    description: "Capture sizing, cash, and asset count.",
  },
  {
    title: "Review and Run",
    description: "Confirm the request before creating a strategy run.",
  },
];

const defaultState: AllocationWizardState = {
  universe: "top25",
  exclusions: ["stablecoins", "wrapped"],
  minimumMarketCap: "1b",
  concentrationLimit: "20",
  maxDrawdown: "35",
  riskPreference: "balanced",
  horizon: "1y",
  rebalance: "monthly",
  initialCapitalUsd: "",
  cashAllocation: "10",
  targetAssets: "5-10",
};

const initialCapitalInvalidCode = "initial_capital_invalid";

type NavigationSource =
  | "next"
  | "back"
  | "sidebar"
  | "summary"
  | "review_button";

type ValidationResult = {
  messages: string[];
  codes: string[];
};

const universeOptions: Option<AllocationWizardState["universe"]>[] = [
  { value: "top10", label: "Top 10 by market cap" },
  { value: "top25", label: "Top 25 by market cap" },
  { value: "top50", label: "Top 50 by market cap" },
  { value: "all", label: "All available assets" },
];

const exclusionOptions: Option[] = [
  { value: "stablecoins", label: "Stablecoins" },
  { value: "wrapped", label: "Wrapped assets" },
];

const minimumMarketCapOptions: Option<
  AllocationWizardState["minimumMarketCap"]
>[] = [
  { value: "none", label: "No minimum" },
  { value: "100m", label: "$100M+" },
  { value: "500m", label: "$500M+" },
  { value: "1b", label: "$1B+" },
  { value: "10b", label: "$10B+" },
];

const concentrationLimitOptions: Option<
  AllocationWizardState["concentrationLimit"]
>[] = [
  { value: "20", label: "Max 20% per token" },
  { value: "30", label: "Max 30% per token" },
  { value: "agent", label: "No hard cap, agent decides" },
];

const maxDrawdownOptions: Option<AllocationWizardState["maxDrawdown"]>[] = [
  { value: "10", label: "10%" },
  { value: "20", label: "20%" },
  { value: "35", label: "35%" },
  { value: "50", label: "50%" },
  { value: "moreThan50", label: "More than 50%" },
];

const riskPreferenceOptions: Option<AllocationWizardState["riskPreference"]>[] =
  [
    { value: "preserve", label: "Preserve capital" },
    { value: "balanced", label: "Balanced growth" },
    { value: "aggressive", label: "Aggressive growth" },
    { value: "maxUpside", label: "Maximum upside" },
  ];

const horizonOptions: Option<AllocationWizardState["horizon"]>[] = [
  { value: "3m", label: "3 months" },
  { value: "6m", label: "6 months" },
  { value: "1y", label: "1 year" },
  { value: "3yPlus", label: "3+ years" },
];

const rebalanceOptions: Option<AllocationWizardState["rebalance"]>[] = [
  { value: "none", label: "None / buy-and-hold" },
  { value: "monthly", label: "Monthly" },
  { value: "weekly", label: "Weekly" },
  { value: "agent", label: "Let agent decide" },
];

const cashAllocationOptions: Option<AllocationWizardState["cashAllocation"]>[] =
  [
    { value: "none", label: "No cash" },
    { value: "10", label: "Up to 10%" },
    { value: "25", label: "Up to 25%" },
    { value: "agent", label: "Let agent decide" },
  ];

const targetAssetOptions: Option<AllocationWizardState["targetAssets"]>[] = [
  { value: "3-5", label: "3-5" },
  { value: "5-10", label: "5-10" },
  { value: "10-20", label: "10-20" },
  { value: "agent", label: "Let agent decide" },
];

function getLabel<T extends string>(options: Option<T>[], value: T) {
  return options.find((option) => option.value === value)?.label ?? value;
}

function validateState(state: AllocationWizardState): ValidationResult {
  const messages: string[] = [];
  const codes: string[] = [];
  const initialCapital = state.initialCapitalUsd.trim();

  if (
    initialCapital &&
    (!Number.isFinite(Number(initialCapital)) || Number(initialCapital) <= 0)
  ) {
    messages.push("Initial capital must be a positive number when provided.");
    codes.push(initialCapitalInvalidCode);
  }

  return { messages, codes };
}

function getStepParams(stepIndex: number) {
  return {
    step_index: stepIndex,
    step_name: steps[stepIndex].title,
  };
}

function getInitialCapitalMetadata(value: string) {
  const trimmed = value.trim();
  const amount = Number(trimmed);

  return {
    initial_capital_provided: trimmed.length > 0,
    initial_capital_valid:
      trimmed.length === 0 || (Number.isFinite(amount) && amount > 0),
  };
}

function OptionGroup<T extends string>({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: Option<T>[];
  value: T;
  onChange: (value: T) => void;
}) {
  return (
    <fieldset className="grid gap-3">
      <legend className="text-sm font-medium">{label}</legend>
      <div className="grid gap-2 sm:grid-cols-2">
        {options.map((option) => {
          const isSelected = option.value === value;

          return (
            <button
              key={option.value}
              type="button"
              className={`rounded-xl border p-3 text-left text-sm transition-colors ${
                isSelected
                  ? "border-primary/70 bg-primary/10 text-foreground shadow-[inset_0_0_0_1px_color-mix(in_oklab,var(--primary)_35%,transparent)]"
                  : "border-border bg-background text-foreground hover:bg-muted"
              }`}
              aria-pressed={isSelected}
              onClick={() => onChange(option.value)}
            >
              <span className="font-medium">{option.label}</span>
              {option.description ? (
                <span
                  className={`mt-1 block text-xs leading-5 ${
                    isSelected ? "text-primary" : "text-muted-foreground"
                  }`}
                >
                  {option.description}
                </span>
              ) : null}
            </button>
          );
        })}
      </div>
    </fieldset>
  );
}

type SummaryItem = {
  label: string;
  value: string;
};

type SummarySection = {
  stepIndex: number;
  title: string;
  description: string;
  items: SummaryItem[];
};

function SummaryRow({ label, value }: SummaryItem) {
  return (
    <div className="grid gap-1 rounded-lg border px-3 py-2 text-sm">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-medium">{value}</span>
    </div>
  );
}

function formatUsd(value: string) {
  const trimmed = value.trim();

  if (!trimmed) {
    return "Agent should reason in percentages only";
  }

  const amount = Number(trimmed);

  if (!Number.isFinite(amount) || amount <= 0) {
    return trimmed;
  }

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(amount);
}

export function AllocationWizard() {
  const router = useRouter();
  const [selectedStepIndex, setSelectedStepIndex] = useState(0);
  const [state, setState] = useState<AllocationWizardState>(defaultState);
  const validation = useMemo(() => validateState(state), [state]);
  const errors = validation.messages;
  const navigationSourceRef = useRef<NavigationSource | undefined>(undefined);
  const progress = Math.round(((selectedStepIndex + 1) / steps.length) * 100);
  const isReviewStep = selectedStepIndex === steps.length - 1;

  const trackSelectedStepViewed = useEffectEvent(() => {
    const navigationSource = navigationSourceRef.current;
    navigationSourceRef.current = undefined;

    trackEvent("wizard_step_viewed", {
      ...getStepParams(selectedStepIndex),
      navigation_source: navigationSource,
    });

    if (selectedStepIndex === steps.length - 1) {
      trackEvent("wizard_review_opened", {
        completed_steps: steps.length - 1,
        has_validation_errors: validation.codes.length > 0,
        initial_capital_provided: getInitialCapitalMetadata(
          state.initialCapitalUsd,
        ).initial_capital_provided,
      });

      if (validation.codes.length > 0) {
        trackEvent("wizard_validation_failed", {
          error_count: validation.codes.length,
          error_codes: validation.codes.join(","),
        });
      }
    }
  });

  useEffect(() => {
    trackEvent("wizard_started", getStepParams(0));
  }, []);

  useEffect(() => {
    trackSelectedStepViewed();
  }, [selectedStepIndex]);

  function updateState(update: Partial<AllocationWizardState>) {
    setState((current) => ({ ...current, ...update }));
  }

  function selectPredefinedField<K extends keyof AllocationWizardState>(
    fieldName: K,
    fieldValue: AllocationWizardState[K],
  ) {
    updateState({ [fieldName]: fieldValue });
    trackEvent("wizard_field_selected", {
      ...getStepParams(selectedStepIndex),
      field_name: fieldName,
      field_value: String(fieldValue),
    });
  }

  function goToStep(stepIndex: number, navigationSource: NavigationSource) {
    navigationSourceRef.current = navigationSource;
    setSelectedStepIndex(stepIndex);
  }

  function goNext() {
    if (selectedStepIndex === steps.length - 2 && errors.length > 0) {
      goToStep(steps.length - 1, "next");
      return;
    }

    goToStep(Math.min(selectedStepIndex + 1, steps.length - 1), "next");
  }

  function toggleExclusion(value: string) {
    const selected = !state.exclusions.includes(value);

    setState((current) => ({
      ...current,
      exclusions: selected
        ? [...current.exclusions, value]
        : current.exclusions.filter((item) => item !== value),
    }));

    trackEvent("wizard_exclusion_toggled", {
      ...getStepParams(selectedStepIndex),
      field_name: "exclusions",
      field_value: value,
      selected,
    });
  }

  function trackInitialCapitalChanged() {
    trackEvent("wizard_initial_capital_changed", {
      ...getStepParams(selectedStepIndex),
      ...getInitialCapitalMetadata(state.initialCapitalUsd),
    });
  }

  function runAllocationAgent() {
    if (errors.length > 0) {
      trackEvent("wizard_validation_failed", {
        error_count: validation.codes.length,
        error_codes: validation.codes.join(","),
      });
      return;
    }

    trackEvent("wizard_run_submitted", {
      universe: state.universe,
      minimumMarketCap: state.minimumMarketCap,
      concentrationLimit: state.concentrationLimit,
      maxDrawdown: state.maxDrawdown,
      riskPreference: state.riskPreference,
      horizon: state.horizon,
      rebalance: state.rebalance,
      cashAllocation: state.cashAllocation,
      targetAssets: state.targetAssets,
      exclusions_count: state.exclusions.length,
      initial_capital_provided: getInitialCapitalMetadata(
        state.initialCapitalUsd,
      ).initial_capital_provided,
    });

    const runId = crypto.randomUUID();
    const storedRun = JSON.stringify({
      state,
    });
    localStorage.setItem(`wizard-run:${runId}`, storedRun);
    sessionStorage.setItem(`wizard-run:${runId}`, storedRun);
    router.push(`/wizard/run?id=${encodeURIComponent(runId)}`);
  }

  const summarySections: SummarySection[] = [
    {
      stepIndex: 0,
      title: "Investable universe",
      description: "Assets the strategy may consider before filters.",
      items: [
        {
          label: "Token universe",
          value: getLabel(universeOptions, state.universe),
        },
        {
          label: "Excluded assets",
          value: state.exclusions.length
            ? state.exclusions
                .map((item) => getLabel(exclusionOptions, item))
                .join(", ")
            : "No exclusions",
        },
      ],
    },
    {
      stepIndex: 1,
      title: "Market filters",
      description: "Market-cap and concentration guardrails.",
      items: [
        {
          label: "Minimum market cap",
          value: getLabel(minimumMarketCapOptions, state.minimumMarketCap),
        },
        {
          label: "Single-token limit",
          value: getLabel(concentrationLimitOptions, state.concentrationLimit),
        },
      ],
    },
    {
      stepIndex: 2,
      title: "Risk tolerance",
      description: "Financial drawdown tolerance and risk posture.",
      items: [
        {
          label: "Maximum acceptable drawdown",
          value: getLabel(maxDrawdownOptions, state.maxDrawdown),
        },
        {
          label: "Risk posture",
          value: getLabel(riskPreferenceOptions, state.riskPreference),
        },
      ],
    },
    {
      stepIndex: 3,
      title: "Time horizon",
      description: "How long the allocation should be designed for.",
      items: [
        {
          label: "Time horizon",
          value: getLabel(horizonOptions, state.horizon),
        },
        {
          label: "Rebalancing cadence",
          value: getLabel(rebalanceOptions, state.rebalance),
        },
      ],
    },
    {
      stepIndex: 4,
      title: "Portfolio constraints",
      description: "Sizing, cash, and portfolio breadth assumptions.",
      items: [
        { label: "Initial capital", value: formatUsd(state.initialCapitalUsd) },
        {
          label: "Cash allocation",
          value: getLabel(cashAllocationOptions, state.cashAllocation),
        },
        {
          label: "Target asset count",
          value: getLabel(targetAssetOptions, state.targetAssets),
        },
      ],
    },
  ];

  return (
    <main className="min-h-dvh bg-[radial-gradient(circle_at_top_left,color-mix(in_oklab,var(--primary)_16%,transparent),transparent_34%),linear-gradient(180deg,var(--background),color-mix(in_oklab,var(--accent)_38%,var(--background)))] px-3 py-4 sm:px-4 lg:px-6">
      <div className="mx-auto flex w-full max-w-7xl flex-col gap-4">
        <section className="grid gap-5 overflow-hidden rounded-2xl border bg-background p-4 shadow-sm lg:grid-cols-[1fr_auto] lg:items-end">
          <div className="min-w-0">
            <div className="max-w-3xl space-y-2">
              <Badge variant="secondary" className="w-fit">
                Pond3r agent
              </Badge>
              <h1 className="text-2xl font-semibold tracking-tight sm:text-3xl lg:text-4xl">
                DeFi allocation copilot for crypto portfolio research.
              </h1>
              <p className="text-sm leading-6 text-muted-foreground sm:text-base">
                Give Pond3r the core allocation inputs, review the mandate, and
                launch an agent run grounded in portfolio constraints and
                backtests.
              </p>
            </div>
          </div>
          <div className="grid gap-2 rounded-xl bg-accent p-3 text-sm lg:min-w-60">
            <div className="flex items-center justify-between gap-3">
              <span className="text-muted-foreground">Progress</span>
              <span className="font-medium">
                Step {selectedStepIndex + 1} of {steps.length}
              </span>
            </div>
            <div className="h-2 overflow-hidden rounded-full bg-background">
              <div
                className="h-full rounded-full bg-primary"
                style={{ width: `${progress}%` }}
              />
            </div>
          </div>
        </section>

        <section className="grid min-w-0 gap-4 lg:grid-cols-[18rem_minmax(0,1fr)] xl:grid-cols-[18rem_minmax(0,1fr)_20rem]">
          <Card className="min-w-0 lg:sticky lg:top-4 lg:max-h-[calc(100dvh-2rem)]">
            <CardHeader>
              <CardTitle>Guided steps</CardTitle>
              <CardDescription>Jump back to edit any answer.</CardDescription>
            </CardHeader>
            <CardContent className="min-h-0">
              <ScrollArea className="h-auto lg:max-h-[calc(100dvh-10rem)]">
                <ol className="grid gap-2 pr-1">
                  {steps.map((step, index) => {
                    const isSelected = index === selectedStepIndex;

                    return (
                      <li key={step.title}>
                        <button
                          type="button"
                          className={`flex w-full min-w-0 gap-2.5 rounded-lg border px-3 py-2.5 text-left transition-colors ${
                            isSelected
                              ? "border-primary bg-primary text-primary-foreground shadow-sm"
                              : "border-border bg-background text-foreground hover:border-foreground/20 hover:bg-muted/70"
                          }`}
                          aria-current={isSelected ? "step" : undefined}
                          onClick={() => goToStep(index, "sidebar")}
                        >
                          <span
                            className={`flex size-7 shrink-0 items-center justify-center rounded-full text-xs font-bold ${
                              isSelected
                                ? "bg-primary-foreground text-primary"
                                : "bg-muted text-foreground"
                            }`}
                          >
                            {index + 1}
                          </span>
                          <span className="min-w-0 space-y-1">
                            <span className="block text-sm font-semibold leading-5 tracking-tight">
                              {step.title}
                            </span>
                            <span
                              className={`block text-[13px] leading-5 ${
                                isSelected
                                  ? "text-primary-foreground/90"
                                  : "text-muted-foreground"
                              }`}
                            >
                              {step.description}
                            </span>
                          </span>
                        </button>
                      </li>
                    );
                  })}
                </ol>
              </ScrollArea>
            </CardContent>
          </Card>

          <Card className="min-w-0">
            <CardHeader className="sticky top-0 z-10 border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/80">
              <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                <div className="min-w-0 space-y-2">
                  <Badge variant="outline" className="w-fit">
                    Step {selectedStepIndex + 1}
                  </Badge>
                  <CardTitle className="text-2xl">
                    {steps[selectedStepIndex].title}
                  </CardTitle>
                  <CardDescription>
                    {steps[selectedStepIndex].description}
                  </CardDescription>
                </div>
                <div className="flex shrink-0 flex-col gap-2 sm:flex-row lg:pt-1">
                  <Button
                    variant="outline"
                    size="lg"
                    disabled={selectedStepIndex === 0}
                    className="sm:min-w-28"
                    onClick={() =>
                      goToStep(Math.max(selectedStepIndex - 1, 0), "back")
                    }
                  >
                    Back
                  </Button>
                  {isReviewStep ? (
                    <Button
                      size="lg"
                      disabled={errors.length > 0}
                      className="sm:min-w-44"
                      onClick={runAllocationAgent}
                    >
                      Run allocation agent
                    </Button>
                  ) : (
                    <Button size="lg" className="sm:min-w-52" onClick={goNext}>
                      Continue to {steps[selectedStepIndex + 1].title}
                    </Button>
                  )}
                </div>
              </div>
            </CardHeader>
            <CardContent className="grid gap-5">
              {selectedStepIndex === 0 ? (
                <>
                  <OptionGroup
                    label="Token universe"
                    options={universeOptions}
                    value={state.universe}
                    onChange={(universe) =>
                      selectPredefinedField("universe", universe)
                    }
                  />
                  <fieldset className="grid gap-3">
                    <legend className="text-sm font-medium">Exclusions</legend>
                    <div className="grid gap-2 sm:grid-cols-2">
                      {exclusionOptions.map((option) => {
                        const isSelected = state.exclusions.includes(
                          option.value,
                        );

                        return (
                          <button
                            key={option.value}
                            type="button"
                            className={`rounded-xl border p-3 text-left text-sm transition-colors ${
                              isSelected
                                ? "border-primary/70 bg-primary/10 text-foreground shadow-[inset_0_0_0_1px_color-mix(in_oklab,var(--primary)_35%,transparent)]"
                                : "border-border bg-background text-foreground hover:bg-muted"
                            }`}
                            aria-pressed={isSelected}
                            onClick={() => toggleExclusion(option.value)}
                          >
                            {option.label}
                          </button>
                        );
                      })}
                    </div>
                  </fieldset>
                </>
              ) : null}

              {selectedStepIndex === 1 ? (
                <>
                  <OptionGroup
                    label="Minimum market cap"
                    options={minimumMarketCapOptions}
                    value={state.minimumMarketCap}
                    onChange={(minimumMarketCap) =>
                      selectPredefinedField(
                        "minimumMarketCap",
                        minimumMarketCap,
                      )
                    }
                  />
                  <OptionGroup
                    label="Concentration limit"
                    options={concentrationLimitOptions}
                    value={state.concentrationLimit}
                    onChange={(concentrationLimit) =>
                      selectPredefinedField(
                        "concentrationLimit",
                        concentrationLimit,
                      )
                    }
                  />
                </>
              ) : null}

              {selectedStepIndex === 2 ? (
                <>
                  <OptionGroup
                    label="Maximum financially acceptable drawdown"
                    options={maxDrawdownOptions}
                    value={state.maxDrawdown}
                    onChange={(maxDrawdown) =>
                      selectPredefinedField("maxDrawdown", maxDrawdown)
                    }
                  />
                  <OptionGroup
                    label="Main risk preference"
                    options={riskPreferenceOptions}
                    value={state.riskPreference}
                    onChange={(riskPreference) =>
                      selectPredefinedField("riskPreference", riskPreference)
                    }
                  />
                </>
              ) : null}

              {selectedStepIndex === 3 ? (
                <>
                  <OptionGroup
                    label="Time horizon"
                    options={horizonOptions}
                    value={state.horizon}
                    onChange={(horizon) =>
                      selectPredefinedField("horizon", horizon)
                    }
                  />
                  <OptionGroup
                    label="Rebalancing cadence"
                    options={rebalanceOptions}
                    value={state.rebalance}
                    onChange={(rebalance) =>
                      selectPredefinedField("rebalance", rebalance)
                    }
                  />
                </>
              ) : null}

              {selectedStepIndex === 4 ? (
                <>
                  <div className="grid gap-2">
                    <label
                      className="text-sm font-medium"
                      htmlFor="initial-capital"
                    >
                      Initial capital, optional
                    </label>
                    <Input
                      id="initial-capital"
                      inputMode="decimal"
                      placeholder="10000"
                      value={state.initialCapitalUsd}
                      aria-invalid={
                        !!state.initialCapitalUsd.trim() &&
                        (!Number.isFinite(Number(state.initialCapitalUsd)) ||
                          Number(state.initialCapitalUsd) <= 0)
                      }
                      onChange={(event) =>
                        updateState({ initialCapitalUsd: event.target.value })
                      }
                      onBlur={trackInitialCapitalChanged}
                    />
                    <p className="text-xs leading-5 text-muted-foreground">
                      Leave blank if the agent should reason in percentages
                      only.
                    </p>
                  </div>
                  <OptionGroup
                    label="Cash allocation allowed"
                    options={cashAllocationOptions}
                    value={state.cashAllocation}
                    onChange={(cashAllocation) =>
                      selectPredefinedField("cashAllocation", cashAllocation)
                    }
                  />
                  <OptionGroup
                    label="Target number of assets"
                    options={targetAssetOptions}
                    value={state.targetAssets}
                    onChange={(targetAssets) =>
                      selectPredefinedField("targetAssets", targetAssets)
                    }
                  />
                </>
              ) : null}

              {isReviewStep ? (
                <>
                  {errors.length > 0 ? (
                    <div className="grid gap-2 rounded-xl border border-destructive/50 bg-destructive/10 p-4 text-sm text-destructive">
                      <div className="font-medium">
                        Resolve these before running:
                      </div>
                      {errors.map((error) => (
                        <div key={error}>{error}</div>
                      ))}
                    </div>
                  ) : (
                    <div className="rounded-xl border bg-muted/40 p-4 text-sm leading-6 text-muted-foreground">
                      The mandate is ready for prompt construction in the next
                      implementation step.
                    </div>
                  )}
                  <div className="grid gap-4">
                    {summarySections.map((section) => (
                      <section
                        key={section.title}
                        className="rounded-2xl border bg-background p-4"
                      >
                        <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                          <div className="space-y-1">
                            <h3 className="font-semibold">{section.title}</h3>
                            <p className="text-sm leading-6 text-muted-foreground">
                              {section.description}
                            </p>
                          </div>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() =>
                              goToStep(section.stepIndex, "summary")
                            }
                          >
                            Edit {steps[section.stepIndex].title}
                          </Button>
                        </div>
                        <div className="grid gap-3 sm:grid-cols-2">
                          {section.items.map((item) => (
                            <SummaryRow
                              key={`${section.title}-${item.label}`}
                              label={item.label}
                              value={item.value}
                            />
                          ))}
                        </div>
                      </section>
                    ))}
                  </div>
                </>
              ) : null}
            </CardContent>
          </Card>

          <Card className="min-w-0 lg:col-span-2 lg:sticky lg:top-4 lg:h-fit xl:col-span-1">
            <CardHeader>
              <CardTitle>Review and Run</CardTitle>
              <CardDescription>
                Live summary of the mandate collected so far.
              </CardDescription>
            </CardHeader>
            <CardContent className="grid gap-4">
              <div className="rounded-xl border bg-muted/40 p-4">
                <div className="mb-2 text-sm font-medium">Draft brief</div>
                <p className="text-sm leading-6 text-muted-foreground">
                  Build a{" "}
                  {getLabel(
                    riskPreferenceOptions,
                    state.riskPreference,
                  ).toLowerCase()}{" "}
                  crypto allocation for a{" "}
                  {getLabel(horizonOptions, state.horizon).toLowerCase()}{" "}
                  horizon using{" "}
                  {getLabel(universeOptions, state.universe).toLowerCase()}.
                </p>
              </div>
              <div className="grid gap-2 text-sm">
                {steps.map((step, index) => (
                  <button
                    key={step.title}
                    type="button"
                    className="flex min-w-0 items-center justify-between gap-3 rounded-lg border px-3 py-2 text-left hover:bg-muted"
                    onClick={() => goToStep(index, "summary")}
                  >
                    <span className="truncate">{step.title}</span>
                    <Badge
                      variant={
                        index === selectedStepIndex ? "default" : "outline"
                      }
                    >
                      {index === selectedStepIndex ? "Current" : "Edit"}
                    </Badge>
                  </button>
                ))}
              </div>
            </CardContent>
            <CardFooter className="flex-col items-stretch gap-3">
              <Button
                disabled={errors.length > 0}
                onClick={() => goToStep(steps.length - 1, "review_button")}
              >
                Review mandate
              </Button>
              <p className="text-xs leading-5 text-muted-foreground">
                Strategy creation, prompt construction, and live results will be
                added in later tasks.
              </p>
            </CardFooter>
          </Card>
        </section>
      </div>
    </main>
  );
}
