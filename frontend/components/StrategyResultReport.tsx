import type { ReactNode } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
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
  { key: "calmar_ratio", label: "Calmar", format: formatNumber },
  { key: "monthly_hit_rate", label: "Monthly hit rate", format: formatPercent },
  { key: "final_equity_usd", label: "Final equity", format: formatCurrency },
  {
    key: "total_trading_cost_usd",
    label: "Trading costs",
    format: formatCurrency,
  },
  { key: "total_num_swaps", label: "Swaps", format: formatWholeNumber },
];

const CHART_COLORS = ["#2563eb", "#f97316", "#16a34a", "#9333ea", "#dc2626"];

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

function formatWholeNumber(value: number | null | undefined) {
  if (!hasValue(value)) return "Not provided";
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 0,
  }).format(value);
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

function ChartTooltip({
  active,
  payload,
  label,
  valueFormatter,
}: {
  active?: boolean;
  payload?: Array<{
    color?: string;
    dataKey?: string | number;
    name?: string | number;
    value?: unknown;
  }>;
  label?: string | number;
  valueFormatter: (value: number) => string;
}) {
  if (!active || !payload?.length) return null;
  const title = label ?? payload[0]?.name;

  return (
    <div className="rounded-xl border bg-background/95 p-3 text-sm shadow-sm">
      {title ? (
        <div className="mb-2 font-medium">{formatDate(String(title))}</div>
      ) : null}
      <div className="space-y-1">
        {payload.map((item, index) =>
          hasChartValue(item.value) ? (
            <div
              key={`${item.dataKey ?? item.name}-${index}`}
              className="flex items-center gap-2"
            >
              <span
                className="h-2.5 w-2.5 rounded-full"
                style={{ backgroundColor: item.color }}
              />
              <span className="text-muted-foreground">{item.name}</span>
              <span className="font-medium">{valueFormatter(item.value)}</span>
            </div>
          ) : null,
        )}
      </div>
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
    <div className="h-[320px]">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={chartData} margin={{ left: 0, right: 16, top: 10 }}>
          <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
          <XAxis dataKey="date" tickFormatter={formatDate} minTickGap={28} />
          <YAxis
            tickFormatter={(value) => formatCurrency(Number(value))}
            width={82}
          />
          <Tooltip content={<ChartTooltip valueFormatter={formatCurrency} />} />
          <Legend />
          <Area
            type="monotone"
            dataKey="strategy"
            name="Strategy"
            stroke="#2563eb"
            fill="#2563eb"
            fillOpacity={0.16}
            connectNulls
          />
          <Line
            type="monotone"
            dataKey="bitcoin"
            name="Bitcoin"
            stroke="#f97316"
            strokeWidth={2}
            dot={false}
            connectNulls
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
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
    <div className="h-[320px]">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={chartData} margin={{ left: 0, right: 16, top: 10 }}>
          <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
          <XAxis dataKey="date" tickFormatter={formatDate} minTickGap={28} />
          <YAxis
            tickFormatter={(value) => formatPercent(Number(value))}
            width={64}
          />
          <Tooltip content={<ChartTooltip valueFormatter={formatPercent} />} />
          <Legend />
          <Area
            type="monotone"
            dataKey="strategy"
            name="Strategy"
            stroke="#2563eb"
            fill="#2563eb"
            fillOpacity={0.14}
            connectNulls
          />
          <Line
            type="monotone"
            dataKey="bitcoin"
            name="Bitcoin"
            stroke="#f97316"
            strokeWidth={2}
            dot={false}
            connectNulls
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

function AllocationChart({
  data,
}: {
  data: StrategyChartAllocationItem[] | null;
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
    <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_220px] lg:items-center">
      <div className="h-[260px]">
        <ResponsiveContainer width="100%" height="100%">
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
              content={<ChartTooltip valueFormatter={formatPercent} />}
            />
          </PieChart>
        </ResponsiveContainer>
      </div>
      <div className="space-y-2">
        {chartData.map((item, index) => (
          <div
            key={item.asset}
            className="flex items-center justify-between gap-3 text-sm"
          >
            <div className="flex min-w-0 items-center gap-2">
              <span
                className="h-2.5 w-2.5 shrink-0 rounded-full"
                style={{
                  backgroundColor: CHART_COLORS[index % CHART_COLORS.length],
                }}
              />
              <span className="truncate">{item.asset}</span>
            </div>
            <span className="font-medium">{formatPercent(item.weight)}</span>
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
    <section className="space-y-3 rounded-2xl border bg-background p-4 sm:p-5">
      <h2 className="text-lg font-semibold tracking-tight">{title}</h2>
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
        <li key={`${item}-${index}`} className="rounded-xl bg-muted/40 p-3">
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
  const chartAllocation = charts.allocation ?? allocation;

  return (
    <Card className="overflow-hidden">
      <CardHeader className="border-b">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div className="space-y-2">
            <Badge variant="secondary" className="w-fit">
              Structured strategy report
            </Badge>
            <CardTitle className="text-2xl sm:text-3xl">
              {formatMissing(result.title)}
            </CardTitle>
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-5 p-4 sm:p-6">
        <p className="text-base leading-7 text-muted-foreground">
          {formatMissing(result.summary)}
        </p>

        <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {KPI_ITEMS.map((item) => (
            <div key={item.key} className="rounded-2xl border bg-muted/25 p-4">
              <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                {item.label}
              </div>
              <div className="mt-2 text-2xl font-semibold">
                {item.format(result.kpis?.[item.key])}
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
          <AllocationChart data={chartAllocation} />
          {allocation.length > 0 ? (
            <div className="mt-4 space-y-3">
              {allocation.map((item, index) => (
                <div
                  key={`${item.asset}-${item.symbol ?? index}`}
                  className="rounded-xl border bg-muted/20 p-4"
                >
                  <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                    <div>
                      <h3 className="font-medium">
                        {formatMissing(item.asset)}
                      </h3>
                      <p className="text-xs text-muted-foreground">
                        {item.symbol ?? item.coin_id ?? "No symbol provided"}
                      </p>
                    </div>
                    <Badge variant="outline">
                      {formatPercent(item.weight)}
                    </Badge>
                  </div>
                  <p className="mt-3 text-sm leading-6 text-muted-foreground">
                    {formatMissing(item.rationale)}
                  </p>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">Not provided</p>
          )}
        </ReportSection>

        <ReportSection title="Backtest context">
          <dl className="grid gap-3 text-sm sm:grid-cols-2 lg:grid-cols-4">
            <div className="rounded-xl bg-muted/40 p-3">
              <dt className="text-muted-foreground">Period</dt>
              <dd className="mt-1 font-medium">
                {formatMissing(backtest.start_date)} to{" "}
                {formatMissing(backtest.end_date)}
              </dd>
            </div>
            <div className="rounded-xl bg-muted/40 p-3">
              <dt className="text-muted-foreground">Rebalance</dt>
              <dd className="mt-1 font-medium">
                {formatMissing(backtest.rebalance)}
              </dd>
            </div>
            <div className="rounded-xl bg-muted/40 p-3">
              <dt className="text-muted-foreground">Initial capital</dt>
              <dd className="mt-1 font-medium">
                {formatCurrency(backtest.initial_capital_usd)}
              </dd>
            </div>
            <div className="rounded-xl bg-muted/40 p-3">
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
