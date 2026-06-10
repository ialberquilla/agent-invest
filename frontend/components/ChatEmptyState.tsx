"use client";

import { useState } from "react";
import { Compass, Sparkles } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

type LauncherOption = {
  title: string;
  subtitle: string;
  badge: string;
  prompt?: string;
  action?: "wizard";
};

type LauncherLane = {
  id: string;
  title: string;
  subtitle: string;
  options: LauncherOption[];
};

// Progressive prompt launcher: first choose the capability, then a concrete
// prompt. The copy names product capabilities, not internal tool names.
const lanes: LauncherLane[] = [
  {
    id: "deployable",
    title: "Build a deployable backtest",
    subtitle: "template-backed strategy runs",
    options: [
      {
        title: "Diversified basket",
        subtitle: "multi-asset long allocation",
        badge: "Deployable template",
        prompt:
          "Build a balanced diversified crypto basket from GMX-tradeable assets, target a 1-year horizon, cap drawdown around 35%, and backtest it through the deterministic strategy pipeline.",
      },
      {
        title: "Single-asset trend",
        subtitle: "one market, long/flat",
        badge: "Deployable template",
        prompt:
          "Backtest a single-asset SOL trend setup using the supported deterministic template. Keep it long/flat, explain the tested signal assumptions, and only recommend it if validation passes.",
      },
      {
        title: "Pair trade",
        subtitle: "long one market, short another",
        badge: "Deployable template",
        prompt:
          "Backtest a deterministic pair trade: long ETH and short BTC. Use the supported pair-trade template, test the predefined hedge-ratio variants, and disclose short/funding-cost caveats.",
      },
      {
        title: "Long/short momentum",
        subtitle: "strongest long, weakest short",
        badge: "Deployable template",
        prompt:
          "Build a market-neutral long/short momentum strategy over GMX-tradeable assets. Use supported deterministic templates, validate exposure and beta, and disclose short/funding-cost caveats.",
      },
    ],
  },
  {
    id: "research",
    title: "Explore a custom research idea",
    subtitle: "flexible but research-only",
    options: [
      {
        title: "Test a custom signal",
        subtitle: "describe rules in plain English",
        badge: "Research-only",
        prompt:
          "Run an exploratory research-only backtest for this idea: SOL long when 20-day momentum is positive, flat otherwise. Compare it against buy-and-hold and explain whether it could later become a supported deterministic template.",
      },
      {
        title: "Compare two ideas",
        subtitle: "side-by-side research",
        badge: "Research-only",
        prompt:
          "Research whether a single-asset trend-following setup or a diversified momentum basket has historically behaved better for drawdown-adjusted returns. Treat this as exploratory only, not deployable yet.",
      },
      {
        title: "Check a thesis",
        subtitle: "did the idea hold historically?",
        badge: "Research-only",
        prompt:
          "Research this thesis: ETH outperforms BTC during broad crypto uptrends but underperforms in drawdowns. Use available historical data, show evidence, and say what template-backed strategy could test it next.",
      },
    ],
  },
  {
    id: "screen",
    title: "Screen markets",
    subtitle: "discover GMX-tradeable candidates",
    options: [
      {
        title: "Momentum leaders",
        subtitle: "rank strong GMX markets",
        badge: "Market screening",
        prompt:
          "Screen GMX-tradeable markets for the strongest recent momentum candidates. Return the ranked list and explain which names are worth considering for a deterministic backtest.",
      },
      {
        title: "Lower-drawdown assets",
        subtitle: "defensive candidate set",
        badge: "Market screening",
        prompt:
          "Screen GMX-tradeable markets for relatively lower drawdown and steadier trend behavior. Return candidates that could seed a conservative basket backtest.",
      },
      {
        title: "Pair-trade candidates",
        subtitle: "look for relative-value pairs",
        badge: "Market screening",
        prompt:
          "Screen GMX-tradeable markets for possible pair-trade candidates. Look for liquid large-cap pairs with a plausible relative-value thesis, then suggest one deterministic pair backtest to run next.",
      },
    ],
  },
  {
    id: "beginner",
    title: "Start beginner-friendly",
    subtitle: "guided setup and basics",
    options: [
      {
        title: "Open guided setup",
        subtitle: "step-by-step basket wizard",
        badge: "Guided setup",
        action: "wizard",
      },
      {
        title: "Explain the workflow",
        subtitle: "what backtests can and cannot do",
        badge: "Learn",
        prompt:
          "Explain how this crypto strategy copilot works: deterministic backtests, research-only custom analysis, market screening, max drawdown, and what can become deployable.",
      },
      {
        title: "Balanced starter brief",
        subtitle: "simple first portfolio",
        badge: "Deployable template",
        prompt:
          "Create a beginner-friendly balanced crypto basket from GMX-tradeable assets, explain the assumptions, backtest it, and keep the recommendation educational rather than financial advice.",
      },
    ],
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
  const [selectedLaneId, setSelectedLaneId] = useState(lanes[0].id);
  const selectedLane =
    lanes.find((lane) => lane.id === selectedLaneId) ?? lanes[0];

  function runOption(option: LauncherOption) {
    if (option.action === "wizard") {
      onOpenWizard();
      return;
    }
    if (option.prompt) onSelectPrompt(option.prompt);
  }

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-1 flex-col items-center justify-center gap-8 py-12">
      <div className="space-y-2 text-center">
        <div className="mx-auto flex size-11 items-center justify-center rounded-2xl bg-primary/10 text-primary">
          <Sparkles className="size-5" />
        </div>
        <h2 className="font-heading text-xl font-semibold tracking-tight">
          What should we build?
        </h2>
        <p className="text-sm leading-6 text-muted-foreground">
          Choose a path to see what the copilot can do: deployable templates,
          research-only custom ideas, market screens, or reruns.
        </p>
      </div>

      <div className="grid w-full gap-3 lg:grid-cols-[0.9fr_1.4fr]">
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-1">
          {lanes.map((lane) => {
            const active = lane.id === selectedLane.id;
            return (
              <button
                key={lane.id}
                type="button"
                disabled={disabled}
                onClick={() => setSelectedLaneId(lane.id)}
                className={`rounded-xl border px-4 py-3 text-left transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-60 ${
                  active
                    ? "border-primary bg-primary/5 ring-1 ring-primary/30"
                    : "border-border/70 bg-card hover:bg-accent/50"
                }`}
              >
                <span className="block text-sm font-semibold text-foreground">
                  {lane.title}
                </span>
                <span className="mt-0.5 block text-xs text-muted-foreground">
                  {lane.subtitle}
                </span>
              </button>
            );
          })}
        </div>

        <div className="rounded-2xl border border-border/70 bg-card p-3 shadow-sm">
          <div className="mb-3 px-1">
            <h3 className="font-heading text-sm font-semibold tracking-tight">
              {selectedLane.title}
            </h3>
            <p className="text-xs text-muted-foreground">
              Pick a concrete starter prompt. You can edit the idea in chat
              after it starts.
            </p>
          </div>
          <div className="grid gap-2 sm:grid-cols-2">
            {selectedLane.options.map((option) => (
              <button
                key={option.title}
                type="button"
                disabled={disabled}
                onClick={() => runOption(option)}
                className="rounded-xl border border-border/70 bg-background px-4 py-3 text-left transition-colors hover:bg-accent/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-60"
              >
                <Badge variant="outline" className="mb-2">
                  {option.badge}
                </Badge>
                <span className="block text-sm font-medium text-foreground">
                  {option.title}
                </span>
                <span className="mt-0.5 block text-xs text-muted-foreground">
                  {option.subtitle}
                </span>
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="flex w-full flex-wrap justify-center gap-2">
        {[
          "single-asset trend",
          "pair trade",
          "long/short momentum",
          "custom research",
          "GMX screeners",
        ].map((label) => (
          <button
            key={label}
            type="button"
            disabled={disabled}
            onClick={() =>
              onSelectPrompt(`Show me what is possible with ${label}.`)
            }
            className="rounded-full border border-border/70 bg-background px-3 py-1.5 text-xs text-muted-foreground transition-colors hover:bg-accent/50 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-60"
          >
            {label}
          </button>
        ))}
      </div>

      <div className="flex flex-col items-center gap-2.5">
        <p className="text-xs text-muted-foreground">Not sure where to start?</p>
        <Button
          type="button"
          size="lg"
          disabled={disabled}
          onClick={onOpenWizard}
          className="gap-2 px-6 shadow-lg shadow-primary/25 ring-1 ring-primary/40 transition-transform hover:scale-[1.02]"
        >
          <Compass className="size-4" />
          Try the guided setup
        </Button>
      </div>
    </div>
  );
}
