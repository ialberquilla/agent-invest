import * as React from "react";

import { cn } from "@/lib/utils";

function ChartContainer({
  className,
  children,
  ...props
}: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="chart"
      className={cn(
        "flex aspect-video justify-center text-xs text-chart-legend [&_.recharts-cartesian-axis-tick_text]:fill-chart-axis [&_.recharts-curve.recharts-tooltip-cursor]:stroke-[var(--chart-grid)] [&_.recharts-dot[stroke='#fff']]:stroke-transparent [&_.recharts-layer]:outline-hidden [&_.recharts-legend-item-text]:!text-chart-legend [&_.recharts-sector]:outline-hidden [&_.recharts-surface]:outline-hidden",
        className,
      )}
      {...props}
    >
      {children}
    </div>
  );
}

function ChartTooltipContent({
  active,
  payload,
  label,
  labelFormatter,
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
  labelFormatter?: (value: string) => string;
  valueFormatter: (value: number) => string;
}) {
  if (!active || !payload?.length) return null;

  return (
    <div className="grid min-w-32 gap-1.5 rounded-lg border bg-popover/95 px-2.5 py-2 text-xs text-popover-foreground shadow-xl shadow-primary/10 backdrop-blur">
      {label ? (
        <div className="font-medium text-popover-foreground">
          {labelFormatter ? labelFormatter(String(label)) : label}
        </div>
      ) : null}
      <div className="grid gap-1.5">
        {payload.map((item, index) => {
          const value = item.value;
          if (typeof value !== "number" || !Number.isFinite(value)) return null;

          return (
            <div
              key={`${item.dataKey ?? item.name}-${index}`}
              className="flex items-center gap-2"
            >
              <span
                className="size-2.5 shrink-0 rounded-[2px]"
                style={{ backgroundColor: item.color }}
              />
              <span className="min-w-0 flex-1 truncate text-chart-legend">
                {item.name}
              </span>
              <span className="font-mono font-medium text-popover-foreground">
                {valueFormatter(value)}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export { ChartContainer, ChartTooltipContent };
