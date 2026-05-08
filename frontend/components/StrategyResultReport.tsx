import type { ReactNode } from "react";
import {
  Area,
  AreaChart,
  Cell,
  Legend,
  Line,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ChartContainer, ChartTooltipContent } from "@/components/ui/chart";
import type {
  StrategyChartAllocationItem,
  StrategyDrawdownPoint,
  StrategyEquityPoint,
  StrategyResult,
} from "@/lib/types";

type StrategyResultReportProps = {
  result: StrategyResult;
};

const KPI_ITEMS: Array<{
  key: keyof StrategyResult["kpis"];
  label: string;
  format: (value: number | null | undefined) => string;
}> = [
  { key: "cagr", label: "CAGR", format: formatPercent },
  { key: "sharpe_ratio", label: "Sharpe", format: formatNumber },
  { key: "sortino_ratio", label: "Sortino", format: formatNumber },
  { key: "max_drawdown", label: "Max drawdown", format: formatPercent },
  { key: "monthly_hit_rate", label: "Monthly hit rate", format: formatPercent },
  { key: "final_equity_usd", label: "Final equity", format: formatCurrency },
];

const CHART_COLORS = [
  "var(--chart-1)",
  "var(--chart-2)",
  "var(--chart-3)",
  "var(--chart-4)",
  "var(--chart-5)",
];

const CHART_AXIS_PROPS = {
  stroke: "#888888",
  fontSize: 12,
  tickLine: false,
  axisLine: false,
} as const;

const STRATEGY_SERIES_COLOR = "#2563eb";
const BITCOIN_SERIES_COLOR = "#f97316";

function hasValue(value: number | null | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function formatMissing(value: string | number | null | undefined) {
  if (value === null || value === undefined || value === "")
    return "Not provided";
  return String(value);
}

function formatPercent(value: number | null | undefined) {
  if (!hasValue(value)) return "Not provided";
  return new Intl.NumberFormat("en-US", {
    style: "percent",
    maximumFractionDigits: 2,
  }).format(value);
}

function formatCurrency(value: number | null | undefined) {
  if (!hasValue(value)) return "Not provided";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatNumber(value: number | null | undefined) {
  if (!hasValue(value)) return "Not provided";
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 2,
  }).format(value);
}

function formatMultiple(value: number | null | undefined) {
  if (!hasValue(value)) return "Not provided";
  return `${new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(value)}x`;
}

function formatDate(value: string | null | undefined) {
  if (!value) return "Not provided";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    year: "numeric",
  }).format(date);
}

function chartNumber(value: number | null | undefined) {
  return hasValue(value) ? value : null;
}

function hasChartValue(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function ChartFallback({ message }: { message: string }) {
  return (
    <div className="flex min-h-[220px] items-center justify-center rounded-xl border border-dashed bg-muted/20 p-6 text-center text-sm text-muted-foreground">
      {message}
    </div>
  );
}

function EquityCurveChart({ data }: { data: StrategyEquityPoint[] | null }) {
  const chartData = (data ?? [])
    .map((point) => ({
      date: point.date,
      strategy: chartNumber(point.strategy_equity),
      bitcoin: chartNumber(point.benchmark_equity),
    }))
    .filter(
      (point) => hasChartValue(point.strategy) || hasChartValue(point.bitcoin),
    );

  if (chartData.length === 0) {
    return <ChartFallback message="Equity curve data was not provided." />;
  }

  return (
    <ChartContainer className="h-[260px] w-full min-w-0 overflow-hidden sm:h-[320px]">
      <ResponsiveContainer width="99%" height="100%">
        <AreaChart data={chartData} margin={{ left: -18, right: 2, top: 10 }}>
          <XAxis
            dataKey="date"
            tickFormatter={formatDate}
            minTickGap={28}
            {...CHART_AXIS_PROPS}
          />
          <YAxis
            tickFormatter={(value) => formatCurrency(Number(value))}
            width={58}
            {...CHART_AXIS_PROPS}
          />
          <Tooltip
            cursor={{ stroke: "var(--border)", strokeDasharray: "3 3" }}
            content={
              <ChartTooltipContent
                labelFormatter={formatDate}
                valueFormatter={formatCurrency}
              />
            }
          />
          <Legend iconType="circle" wrapperStyle={{ fontSize: 12 }} />
          <Area
            type="monotone"
            dataKey="strategy"
            name="Strategy"
            stroke={STRATEGY_SERIES_COLOR}
            fill={STRATEGY_SERIES_COLOR}
            strokeWidth={2.5}
            fillOpacity={0.16}
            connectNulls
          />
          <Line
            type="monotone"
            dataKey="bitcoin"
            name="Bitcoin"
            stroke={BITCOIN_SERIES_COLOR}
            strokeDasharray="6 4"
            strokeWidth={2.5}
            dot={false}
            connectNulls
          />
        </AreaChart>
      </ResponsiveContainer>
    </ChartContainer>
  );
}

function DrawdownChart({ data }: { data: StrategyDrawdownPoint[] | null }) {
  const chartData = (data ?? [])
    .map((point) => ({
      date: point.date,
      strategy: chartNumber(point.strategy_drawdown),
      bitcoin: chartNumber(point.benchmark_drawdown),
    }))
    .filter(
      (point) => hasChartValue(point.strategy) || hasChartValue(point.bitcoin),
    );

  if (chartData.length === 0) {
    return <ChartFallback message="Drawdown data was not provided." />;
  }

  return (
    <ChartContainer className="h-[260px] w-full min-w-0 overflow-hidden sm:h-[320px]">
      <ResponsiveContainer width="99%" height="100%">
        <AreaChart data={chartData} margin={{ left: -12, right: 2, top: 10 }}>
          <XAxis
            dataKey="date"
            tickFormatter={formatDate}
            minTickGap={28}
            {...CHART_AXIS_PROPS}
          />
          <YAxis
            tickFormatter={(value) => formatPercent(Number(value))}
            width={48}
            {...CHART_AXIS_PROPS}
          />
          <Tooltip
            cursor={{ stroke: "var(--border)", strokeDasharray: "3 3" }}
            content={
              <ChartTooltipContent
                labelFormatter={formatDate}
                valueFormatter={formatPercent}
              />
            }
          />
          <Legend iconType="circle" wrapperStyle={{ fontSize: 12 }} />
          <Area
            type="monotone"
            dataKey="strategy"
            name="Strategy"
            stroke={STRATEGY_SERIES_COLOR}
            fill={STRATEGY_SERIES_COLOR}
            strokeWidth={2.5}
            fillOpacity={0.14}
            connectNulls
          />
          <Line
            type="monotone"
            dataKey="bitcoin"
            name="Bitcoin"
            stroke={BITCOIN_SERIES_COLOR}
            strokeDasharray="6 4"
            strokeWidth={2.5}
            dot={false}
            connectNulls
          />
        </AreaChart>
      </ResponsiveContainer>
    </ChartContainer>
  );
}

function AllocationChart({
  data,
  label = "Target allocation",
}: {
  data: StrategyChartAllocationItem[] | null;
  label?: string;
}) {
  const chartData = (data ?? [])
    .map((item) => ({
      asset: item.asset || "Unnamed asset",
      weight: chartNumber(item.weight),
    }))
    .filter((item): item is { asset: string; weight: number } =>
      hasChartValue(item.weight),
    );

  if (chartData.length === 0) {
    return <ChartFallback message="Allocation chart data was not provided." />;
  }

  return (
    <div className="grid min-w-0 gap-4 lg:grid-cols-[minmax(0,1fr)_220px] lg:items-center">
      <div className="sr-only">{label}</div>
      <ChartContainer className="h-[220px] w-full min-w-0 overflow-hidden sm:h-[260px]">
        <ResponsiveContainer width="99%" height="100%">
          <PieChart>
            <Pie
              data={chartData}
              dataKey="weight"
              nameKey="asset"
              innerRadius="54%"
              outerRadius="82%"
              paddingAngle={2}
            >
              {chartData.map((item, index) => (
                <Cell
                  key={item.asset}
                  fill={CHART_COLORS[index % CHART_COLORS.length]}
                />
              ))}
            </Pie>
            <Tooltip
              content={<ChartTooltipContent valueFormatter={formatPercent} />}
            />
          </PieChart>
        </ResponsiveContainer>
      </ChartContainer>
      <div className="min-w-0 space-y-2">
        {chartData.map((item, index) => (
          <div
            key={item.asset}
            className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-3 text-sm"
          >
            <div className="flex min-w-0 items-center gap-2">
              <span
                className="h-2.5 w-2.5 shrink-0 rounded-full"
                style={{
                  backgroundColor: CHART_COLORS[index % CHART_COLORS.length],
                }}
              />
              <span className="min-w-0 truncate">{item.asset}</span>
            </div>
            <span className="shrink-0 font-medium">
              {formatPercent(item.weight)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function ReportSection({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="min-w-0 space-y-3 overflow-hidden rounded-xl border bg-card p-4 shadow-xs sm:p-5">
      <h2 className="font-heading text-base font-semibold tracking-tight">
        {title}
      </h2>
      {children}
    </section>
  );
}

function TextList({ items }: { items: string[] }) {
  if (items.length === 0) {
    return <p className="text-sm text-muted-foreground">Not provided</p>;
  }

  return (
    <ul className="space-y-2 text-sm leading-6">
      {items.map((item, index) => (
        <li key={`${item}-${index}`} className="rounded-lg bg-muted/50 p-3">
          {item}
        </li>
      ))}
    </ul>
  );
}

export function StrategyResultReport({ result }: StrategyResultReportProps) {
  const allocation = result.allocation ?? [];
  const backtest = result.backtest ?? {};
  const charts = result.charts ?? {};
  const chartAllocation =
    charts.target_allocation ?? charts.allocation ?? allocation;
  const finalAllocation = charts.final_allocation ?? null;
  const normalizedCapital = backtest.capital_mode === "normalized";

  return (
    <Card className="overflow-hidden shadow-xs">
      <CardHeader className="border-b">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div className="space-y-2">
            <Badge variant="secondary" className="w-fit">
              Structured strategy report
            </Badge>
            <CardTitle className="font-heading text-2xl font-bold tracking-tight sm:text-3xl">
              {formatMissing(result.title)}
            </CardTitle>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-5 p-4 sm:p-6">
        <p className="text-base leading-7 text-muted-foreground">
          {formatMissing(result.summary)}
        </p>

        <section className="grid gap-3 sm:grid-cols-2 md:grid-cols-3 xl:grid-cols-6">
          {KPI_ITEMS.map((item) => (
            <div
              key={item.key}
              className="rounded-lg border bg-card px-3 py-2.5 shadow-xs"
            >
              <div className="truncate text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
                {item.label}
              </div>
              <div className="mt-1.5 truncate font-heading text-lg font-bold tracking-tight">
                {item.key === "final_equity_usd" && normalizedCapital
                  ? formatMultiple(result.kpis?.final_equity_multiple)
                  : item.format(result.kpis?.[item.key])}
              </div>
            </div>
          ))}
        </section>

        <div className="grid gap-5 xl:grid-cols-2">
          <ReportSection title="Equity curve">
            <EquityCurveChart data={charts.equity_curve ?? null} />
          </ReportSection>
          <ReportSection title="Drawdown">
            <DrawdownChart data={charts.drawdown ?? null} />
          </ReportSection>
        </div>

        <ReportSection title="Allocation">
          <div className="grid gap-5 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)] xl:items-start">
            <div className="space-y-3">
              <div>
                <h3 className="font-heading text-sm font-semibold tracking-tight">
                  Target allocation
                </h3>
                <p className="text-sm text-muted-foreground">
                  The proposed target weights used for the selected backtest.
                </p>
              </div>
              <AllocationChart data={chartAllocation} />
              {finalAllocation ? (
                <div className="rounded-xl border bg-muted/30 p-3">
                  <h3 className="font-heading text-sm font-semibold tracking-tight">
                    Final drifted allocation
                  </h3>
                  <p className="mt-1 text-sm text-muted-foreground">
                    Buy-and-hold weights can drift away from target weights over
                    time.
                  </p>
                  <div className="mt-3">
                    <AllocationChart
                      data={finalAllocation}
                      label="Final drifted allocation"
                    />
                  </div>
                </div>
              ) : null}
            </div>
            <div className="min-w-0 space-y-3">
              <div>
                <h3 className="font-heading text-sm font-semibold tracking-tight">
                  Selected assets
                </h3>
                <p className="text-sm text-muted-foreground">
                  Weights and rationale behind the proposed allocation.
                </p>
              </div>
              {allocation.length > 0 ? (
                <div className="grid max-h-[28rem] gap-3 overflow-auto pr-1">
                  {allocation.map((item, index) => (
                    <div
                      key={`${item.asset}-${item.symbol ?? index}`}
                      className="rounded-xl border bg-card p-3 shadow-xs"
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <h3 className="truncate text-sm font-semibold">
                            {formatMissing(item.asset)}
                          </h3>
                          <p className="text-xs text-muted-foreground">
                            {item.symbol ??
                              item.coin_id ??
                              "No symbol provided"}
                          </p>
                        </div>
                        <Badge variant="outline" className="shrink-0">
                          {formatPercent(item.weight)}
                        </Badge>
                      </div>
                      <p className="mt-2 text-sm leading-6 text-muted-foreground">
                        {formatMissing(item.rationale)}
                      </p>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-sm text-muted-foreground">Not provided</p>
              )}
            </div>
          </div>
        </ReportSection>

        <ReportSection title="Backtest context">
          <dl className="grid gap-3 text-sm sm:grid-cols-2 lg:grid-cols-4">
            <div className="rounded-lg bg-muted/50 p-3">
              <dt className="text-muted-foreground">Period</dt>
              <dd className="mt-1 font-medium">
                {formatMissing(backtest.start_date)} to{" "}
                {formatMissing(backtest.end_date)}
              </dd>
            </div>
            <div className="rounded-lg bg-muted/50 p-3">
              <dt className="text-muted-foreground">Rebalance</dt>
              <dd className="mt-1 font-medium">
                {formatMissing(backtest.rebalance)}
              </dd>
            </div>
            <div className="rounded-lg bg-muted/50 p-3">
              <dt className="text-muted-foreground">Initial capital</dt>
              <dd className="mt-1 font-medium">
                {normalizedCapital
                  ? `${formatNumber(backtest.initial_capital_usd)} normalized units`
                  : formatCurrency(backtest.initial_capital_usd)}
              </dd>
            </div>
            <div className="rounded-lg bg-muted/50 p-3">
              <dt className="text-muted-foreground">Benchmark</dt>
              <dd className="mt-1 font-medium">
                {formatMissing(backtest.benchmark)}
              </dd>
            </div>
          </dl>
        </ReportSection>

        <ReportSection title="Reasoning">
          <p className="text-sm leading-6 text-muted-foreground">
            {formatMissing(result.reasoning)}
          </p>
        </ReportSection>

        {(result.constraint_violations ?? []).length > 0 ? (
          <ReportSection title="Constraint Violations">
            <TextList items={result.constraint_violations ?? []} />
          </ReportSection>
        ) : null}

        <div className="grid gap-5 lg:grid-cols-3">
          <ReportSection title="Assumptions">
            <TextList items={result.assumptions ?? []} />
          </ReportSection>
          <ReportSection title="Risks">
            <TextList items={result.risks ?? []} />
          </ReportSection>
          <ReportSection title="Next steps">
            <TextList items={result.next_steps ?? []} />
          </ReportSection>
        </div>
      </CardContent>
    </Card>
  );
}
