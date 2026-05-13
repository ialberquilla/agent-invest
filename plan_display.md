# Structured Strategy Display Plan

## Goal

Change the strategy creation experience from a permanent two-column layout into a staged flow:

1. While the agent is running, show only the live thinking/activity in a centered column.
2. Once the run finishes, hide the live activity and show only the final strategy result.
3. Render the final result as a structured report instead of raw text.
4. Make chart data parseable so the frontend can render modern interactive charts instead of relying only on generated PNG artifacts.

## Current State

The current wizard run page is implemented in `frontend/components/WizardRunView.tsx`.

It always renders two columns:

- Left column: live agent activity via `LiveActivity`.
- Right column: final response text and artifacts.

The final result currently comes from `runResult.reply`, which is a plain string stored by the backend. The frontend can display it, but it cannot reliably extract KPIs, allocations, assumptions, risks, or chart series from it.

The backend stream endpoint is implemented in `agent/src/api/server.ts`. The final stream event currently sends a `run.completed` payload containing the run fields plus discovered artifacts.

## Desired UX

### Running State

Show a centered single-column progress view.

Content:

- Page title: `The agent is building your strategy.`
- Status badge: `running`
- Live activity feed with reasoning, tool calls, and command outputs.
- No right-side empty result panel.
- No split layout.

Recommended layout:

- `main` remains full-page.
- Inner wrapper uses `max-w-3xl` or `max-w-4xl`.
- Activity card fills the centered column.
- Auto-scroll should continue using the existing `activityEndRef` logic.

### Completed State

Show only the final structured result.

Content:

- Page title: `Your strategy is ready.`
- Summary card.
- KPI cards.
- Allocation section.
- Interactive charts.
- Methodology/reasoning.
- Assumptions.
- Risks.
- Next steps.
- Artifact/download links as secondary supporting material.

The live activity should be removed from the main view after completion. If debug visibility is useful, add a secondary collapsed section or a `View run details` action later, but do not keep the creation logs as a competing column.

### Error State

Show a centered error card.

Content:

- Error message.
- `Back to wizard` action.
- If a partial run exists, optionally show run id and artifacts.

## Structured Result Contract

The frontend should not parse arbitrary markdown. The agent response should include structured data that the backend validates and passes to the frontend.

Recommended schema:

```ts
type StrategyResult = {
  title: string;
  summary: string;
  reasoning: string;
  allocation: StrategyAllocationItem[];
  kpis: StrategyKpis;
  assumptions: string[];
  risks: string[];
  next_steps: string[];
  backtest: StrategyBacktestSummary;
  charts: StrategyCharts;
};

type StrategyAllocationItem = {
  asset: string;
  symbol?: string;
  coin_id?: string;
  weight: number;
  rationale: string;
};

type StrategyKpis = {
  cagr?: number;
  sharpe_ratio?: number;
  sortino_ratio?: number;
  max_drawdown?: number;
  calmar_ratio?: number;
  monthly_hit_rate?: number;
  final_equity_usd?: number;
  total_trading_cost_usd?: number;
  total_num_swaps?: number;
};

type StrategyBacktestSummary = {
  start_date?: string;
  end_date?: string;
  rebalance?: "none" | "daily" | "weekly" | "monthly";
  initial_capital_usd?: number;
  benchmark?: "bitcoin";
};

type StrategyCharts = {
  equity_curve?: Array<{
    date: string;
    strategy_equity: number;
    benchmark_equity?: number;
  }>;
  drawdown?: Array<{
    date: string;
    strategy_drawdown: number;
    benchmark_drawdown?: number;
  }>;
  allocation?: Array<{
    asset: string;
    weight: number;
  }>;
};
```

Field conventions:

- Percent values should be decimals, not display strings. Example: `0.241` means `24.1%`.
- Weights should be decimals and should normally sum to `1.0` or less.
- Dates should use ISO `YYYY-MM-DD` strings.
- Money values should be numbers in USD.
- Missing metrics should be omitted or set to `null`, but the frontend should tolerate both.
- Equity curve and drawdown data should always include Bitcoin as the benchmark when Bitcoin price data is available.

## Agent Output Format

Use a fenced block with a stable language marker so the backend can extract it reliably.

Example final reply format:

````md
Here is the completed strategy report.

```strategy_result
{
  "title": "BTC/ETH Core Momentum Allocation",
  "summary": "A monthly rebalanced BTC/ETH allocation designed to keep exposure concentrated while improving risk-adjusted return versus a single-asset position.",
  "reasoning": "I listed the eligible universe, selected liquid core assets, ran a monthly rebalanced backtest, and chose the final allocation because it improved Sharpe while keeping drawdown within the tested range.",
  "allocation": [
    {
      "asset": "Bitcoin",
      "symbol": "BTC",
      "coin_id": "bitcoin",
      "weight": 0.6,
      "rationale": "Primary store-of-value exposure and deepest liquidity."
    },
    {
      "asset": "Ethereum",
      "symbol": "ETH",
      "coin_id": "ethereum",
      "weight": 0.4,
      "rationale": "Complements BTC with smart-contract platform exposure."
    }
  ],
  "kpis": {
    "cagr": 0.31,
    "sharpe_ratio": 1.14,
    "sortino_ratio": 1.72,
    "max_drawdown": -0.38,
    "calmar_ratio": 0.82,
    "monthly_hit_rate": 0.58,
    "final_equity_usd": 18432.51,
    "total_trading_cost_usd": 92.13,
    "total_num_swaps": 12
  },
  "assumptions": [
    "Backtest uses available historical price data from the local dataset.",
    "Portfolio is rebalanced monthly.",
    "Trading costs use the configured backtest cost model."
  ],
  "risks": [
    "Crypto assets can experience severe drawdowns.",
    "Backtest performance does not guarantee future results.",
    "The model does not account for the user's full financial situation."
  ],
  "next_steps": [
    "Compare against a lower-volatility allocation.",
    "Test a broader basket with stablecoin cash allocation.",
    "Run a longer backtest window if data is available."
  ],
  "backtest": {
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "rebalance": "monthly",
    "initial_capital_usd": 10000,
    "benchmark": "bitcoin"
  },
  "charts": {
    "equity_curve": [
      { "date": "2024-01-01", "strategy_equity": 10000, "benchmark_equity": 10000 },
      { "date": "2024-02-01", "strategy_equity": 10810, "benchmark_equity": 10690 }
    ],
    "drawdown": [
      { "date": "2024-01-01", "strategy_drawdown": 0, "benchmark_drawdown": 0 },
      { "date": "2024-02-01", "strategy_drawdown": -0.04, "benchmark_drawdown": -0.06 }
    ],
    "allocation": [
      { "asset": "Bitcoin", "weight": 0.6 },
      { "asset": "Ethereum", "weight": 0.4 }
    ]
  }
}
```
````

The backend should extract the `strategy_result` block and parse it as JSON.

## Structured Charts

Yes, structured charts are possible and are a strong improvement.

The agent is already using Python scripts for backtesting. Python can produce both static artifacts and machine-readable chart data. The frontend can then render that data with a React chart library.

Recommended approach:

1. Keep generating PNG artifacts for fallback and downloads.
2. Extend the backtest output to include chart series in `report.json`.
3. Always include Bitcoin as the benchmark series for equity curve and drawdown charts when Bitcoin price data is available for the tested window.
4. Have the agent include the relevant series in the structured `strategy_result`, or have the backend read `report.json` directly and attach chart data.
5. Render charts in the frontend with a modern React charting library.

The best source of truth for chart data should be the Python backtest result, not model-written values. The model can summarize and explain, but the numerical chart series should come from `report.json` or another script-generated JSON artifact.

### Recommended Chart Data Sources

Preferred source order:

1. `report.json` generated by `run_backtest`.
2. Additional JSON/CSV artifacts generated by Python, such as `equity_curve.json` and `drawdown.json`.
3. Structured JSON included in the agent final reply.
4. Raw markdown parsing should be avoided.

If the current `report.json` does not include full chart series, update the Python backtest script to write them.

Recommended artifacts:

- `report.json`: KPIs, configuration, metadata, and compact summary.
- `equity_curve.json`: date-indexed strategy equity series plus Bitcoin benchmark equity series.
- `drawdown.json`: date-indexed strategy drawdown series plus Bitcoin benchmark drawdown series.
- `allocation.json`: final allocation weights.

Example `equity_curve.json`:

```json
[
  { "date": "2024-01-01", "strategy_equity": 10000, "benchmark_equity": 10000 },
  {
    "date": "2024-01-02",
    "strategy_equity": 10084.23,
    "benchmark_equity": 10052.11
  }
]
```

In this file, `benchmark_equity` should represent a Bitcoin buy-and-hold benchmark normalized to the same initial capital as the strategy.

Example `drawdown.json`:

```json
[
  { "date": "2024-01-01", "strategy_drawdown": 0, "benchmark_drawdown": 0 },
  {
    "date": "2024-01-02",
    "strategy_drawdown": -0.012,
    "benchmark_drawdown": -0.018
  }
]
```

In this file, `benchmark_drawdown` should represent the drawdown of the normalized Bitcoin benchmark over the same backtest window.

Example `allocation.json`:

```json
[
  { "asset": "Bitcoin", "symbol": "BTC", "coin_id": "bitcoin", "weight": 0.6 },
  { "asset": "Ethereum", "symbol": "ETH", "coin_id": "ethereum", "weight": 0.4 }
]
```

## Chart Library Options

Recommended frontend chart libraries:

- `recharts`: pragmatic, React-friendly, fast to implement, good for dashboard-style KPI pages.
- `nivo`: attractive and feature-rich, but heavier.
- `visx`: very flexible and polished, but requires more custom chart work.
- `echarts-for-react`: powerful and interactive, but larger and visually more generic unless carefully styled.

Recommended choice for this project: `recharts`.

Reasons:

- Easy to render line charts, area charts, and pie/donut charts.
- Works well with simple JSON arrays.
- Good fit for equity curves, drawdowns, and allocation charts.
- Lower implementation cost than `visx`.

Potential charts:

- Equity curve: `ResponsiveContainer` + `AreaChart` or `LineChart`.
- Equity curve benchmark: render the strategy and Bitcoin benchmark together, with Bitcoin styled as a secondary comparison line.
- Drawdown: negative `AreaChart` with strategy and Bitcoin benchmark drawdown series.
- Allocation: donut chart or horizontal weight bars.
- KPI cards: not chart library dependent.

## Backend Implementation Plan

### 1. Define Shared Types

Add a shared type for `StrategyResult` on the frontend, likely in `frontend/lib/types.ts`.

Mirror or separately define a backend validation shape in the agent service. Avoid trusting arbitrary JSON from the model.

### 2. Add Structured Result Parser

In `agent/src/api/server.ts`, add parsing logic that:

- Searches final reply text for a fenced `strategy_result` block.
- Parses the block as JSON.
- Validates the minimum required shape.
- Returns `null` if missing or invalid.

Minimum required fields:

- `title`
- `summary`
- `reasoning`
- `allocation`
- `kpis`
- `assumptions`
- `risks`
- `next_steps`

Validation should be tolerant but safe:

- Unknown fields are allowed.
- Missing optional KPI fields are allowed.
- Wrong top-level field types should invalidate the structured result.

### 3. Prefer Backtest Artifacts For Numerical Data

If possible, enrich the structured result from generated artifacts:

- Read `report.json` from the discovered artifacts.
- Use script-generated KPIs over model-written KPIs.
- Read `equity_curve.json`, `drawdown.json`, and `allocation.json` if available.
- Attach those values to the final `structured_result`.

This reduces hallucination risk and makes charts trustworthy.

### 4. Extend Run Payload

Extend the final run response shape to include:

```ts
structured_result?: StrategyResult | null;
```

This affects:

- `runResponse` in `agent/src/api/server.ts`.
- `run.completed` SSE payload.
- `frontend/lib/types.ts`.
- `frontend/lib/agent-events.ts`.

### 5. Do Not Persist Structured Results Yet

For the first implementation, structured results should not be persisted.

Implementation choice:

- Parse the structured result during request completion.
- Send the parsed `structured_result` in the `run.completed` SSE payload.
- Keep storing only the plain `reply` in the existing run record.
- Do not add a `structured_result` database column yet.
- Do not add a migration for structured result persistence yet.

Tradeoff:

- This keeps the implementation smaller and avoids changing the database schema now.
- Reloaded historical runs may only have the raw `reply`, unless the backend reparses the saved reply later.
- The frontend must keep a fallback path that renders the raw final reply when `structured_result` is missing.

Persistence can be added later if revisitable structured reports become a product requirement.

## Agent Prompt Plan

Update `agent/src/agent/prompt.ts` response policy to require structured output for portfolio strategy creation.

Add instructions:

- For portfolio allocation, strategy design, comparison, or simulation requests, end with a `strategy_result` fenced JSON block.
- The JSON must match the schema.
- Do not include comments inside JSON.
- Use decimals for percentages and weights.
- Put user-facing explanation in `reasoning`.
- Do not invent KPIs; use values from `run_backtest`.
- Reference generated artifacts but do not rely on PNGs as the only chart source.
- Always use Bitcoin as the benchmark for equity curve and drawdown chart data when Bitcoin price data is available.

Also update the tool manifest for `run_backtest` if the Python script is changed to produce chart JSON artifacts.

## Python Backtest Plan

Inspect the current `run_backtest` script and its `report.json` output.

If full series are missing, update the script to write:

- `equity_curve.json`
- `drawdown.json`
- `allocation.json`

The equity and drawdown series should always include Bitcoin as a benchmark when Bitcoin data is present. The benchmark should be normalized to the same `initial_capital_usd` as the tested strategy so the chart can compare both lines directly.

Keep existing outputs:

- `equity_curve.png`
- `drawdown.png`
- `report.json`

The JSON files should be compact enough for frontend rendering. If daily data becomes too large, downsample before sending to the frontend or keep the full artifact available by URL and fetch it separately.

## Frontend Implementation Plan

### 1. Change Wizard Run Layout

Update `frontend/components/WizardRunView.tsx`.

Replace the permanent two-column grid with conditional states:

```tsx
if (!isFinished) {
  return <CenteredLiveRunView />;
}

return <StructuredStrategyResultView />;
```

This does not need to be literally implemented as early returns, but the rendered layout should behave this way.

### 2. Add Structured Result Component

Create a component such as:

```txt
frontend/components/StrategyResultReport.tsx
```

Responsibilities:

- Accept `StrategyResult`.
- Render title and summary.
- Render KPI cards.
- Render allocation section.
- Render charts.
- Render reasoning, assumptions, risks, and next steps.
- Render artifacts as fallback/supporting files.

### 3. Add Chart Components

Create small chart components rather than one large chart file:

- `EquityCurveChart`
- `DrawdownChart`
- `AllocationChart`

These can live inside `StrategyResultReport.tsx` initially if the implementation stays small. Extract only when the file gets too large.

### 4. Install Chart Dependency

If using `recharts`, add it to the frontend package.

```sh
pnpm add recharts
```

Use pnpm for all JavaScript dependency changes in this repo.

### 5. Formatting Helpers

Add local formatting helpers for:

- Percent formatting.
- Currency formatting.
- Number formatting.
- Date label formatting.

Keep these close to the report component unless reused elsewhere.

### 6. Fallback Behavior

If `structured_result` is missing or invalid:

- Render a fallback card with the raw `finalReply`.
- Still show artifacts if available.
- Do not crash the run page.

This is important during rollout because older runs and malformed model responses may not have structured data.

## Testing Plan

### Backend Tests

Add tests for:

- Extracting a valid `strategy_result` block.
- Returning `null` for missing block.
- Returning `null` for invalid JSON.
- Rejecting invalid top-level field types.
- Including `structured_result` in `run.completed` when valid.

Likely test files:

- `agent/test/server.test.ts`
- A new parser-specific test if parser is extracted.

### Frontend Tests

If the project has frontend test infrastructure, add tests for:

- Running state renders centered live activity only.
- Completed state renders structured report only.
- Raw reply fallback renders when `structured_result` is absent.
- KPI formatting handles missing values.

If no frontend tests exist, manually verify in browser.

### Manual Verification

Run the wizard flow and verify:

- During execution, only centered activity is visible.
- After completion, activity disappears and result report appears.
- KPIs are populated.
- Allocation weights render correctly.
- Charts render from structured series.
- PNG artifacts remain available.
- Error states still work.

## Rollout Strategy

Recommended incremental rollout:

1. Update UI layout to switch between running and completed states.
2. Add structured result parsing and frontend fallback rendering.
3. Add prompt instructions for `strategy_result` JSON.
4. Add structured report UI without charts first.
5. Extend Python backtest artifacts with chart JSON.
6. Add frontend chart components.
7. Persist structured results if revisiting historical reports is required.

This sequence keeps each step small and reduces the chance of breaking the existing run flow.

## Open Questions

1. Should completed runs keep a hidden/collapsible activity log for debugging?
2. Should structured results be persisted in the database immediately?
3. Should chart data be embedded in `run.completed`, fetched lazily from artifact JSON files, or both?
4. Should the result page compare against a benchmark by default?
5. What is the maximum acceptable size for chart data in the SSE final payload?

## Recommendation

Implement this as a structured report system, not just a UI rearrangement.

The layout change is simple, but the real value comes from creating a reliable contract between the agent, backend, and frontend. For charts, the best design is to have Python generate numerical JSON artifacts and let the frontend render them with `recharts`. Static PNGs can remain as downloadable fallback artifacts, but the primary user experience should use interactive frontend charts.
