"use client";

import { Compass, Sparkles } from "lucide-react";

import { Button } from "@/components/ui/button";

type Suggestion = {
  title: string;
  subtitle: string;
  prompt: string;
};

// Tailored to the allocation copilot: each prompt is a complete brief the
// agent can research, design, and backtest without further clarification.
const suggestions: Suggestion[] = [
  {
    title: "Build a balanced portfolio",
    subtitle: "top-25 assets, 1-year horizon",
    prompt:
      "Build a balanced crypto allocation from the top 25 assets by market cap for a 1-year horizon, with no single token above 20%.",
  },
  {
    title: "Aim to beat Bitcoin",
    subtitle: "aggressive growth, 6 months",
    prompt:
      "Design an aggressive growth crypto allocation that tries to outperform Bitcoin over a 6-month horizon, and backtest it.",
  },
  {
    title: "Play it safe",
    subtitle: "preserve capital, low drawdown",
    prompt:
      "Create a capital-preserving crypto allocation that keeps the maximum drawdown around 20% with no single token above 20%.",
  },
  {
    title: "Explain the basics",
    subtitle: "backtests & max drawdown",
    prompt:
      "Explain how backtesting and maximum drawdown work when designing a crypto portfolio.",
  },
];

type ChatEmptyStateProps = {
  disabled?: boolean;
  onSelectPrompt: (prompt: string) => void;
  onOpenWizard: () => void;
};

export function ChatEmptyState({
  disabled = false,
  onSelectPrompt,
  onOpenWizard,
}: ChatEmptyStateProps) {
  return (
    <div className="mx-auto flex w-full max-w-2xl flex-1 flex-col items-center justify-center gap-8 py-16">
      <div className="space-y-2 text-center">
        <div className="mx-auto flex size-11 items-center justify-center rounded-2xl bg-primary/10 text-primary">
          <Sparkles className="size-5" />
        </div>
        <h2 className="font-heading text-xl font-semibold tracking-tight">
          What should we build?
        </h2>
        <p className="text-sm leading-6 text-muted-foreground">
          Describe a crypto allocation and the agent will research, design, and
          backtest it for you.
        </p>
      </div>

      <div className="grid w-full gap-2 sm:grid-cols-2">
        {suggestions.map((suggestion) => (
          <button
            key={suggestion.title}
            type="button"
            disabled={disabled}
            onClick={() => onSelectPrompt(suggestion.prompt)}
            className="rounded-xl border border-border/70 bg-card px-4 py-3 text-left transition-colors hover:bg-accent/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-60"
          >
            <span className="block text-sm font-medium text-foreground">
              {suggestion.title}
            </span>
            <span className="mt-0.5 block text-xs text-muted-foreground">
              {suggestion.subtitle}
            </span>
          </button>
        ))}
      </div>

      <div className="flex flex-col items-center gap-2">
        <p className="text-xs text-muted-foreground">Not sure where to start?</p>
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={disabled}
          onClick={onOpenWizard}
          className="gap-2"
        >
          <Compass className="size-4" />
          Try the guided setup
        </Button>
      </div>
    </div>
  );
}
