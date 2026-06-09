"use client";

import { useEffect, useState } from "react";
import { usePrivy, useWallets } from "@privy-io/react-auth";
import { ArrowUpDown, Pin, RefreshCw, TrendingDown, TrendingUp } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Input } from "@/components/ui/input";
import { createDirectGmxMarketIncreaseOrder } from "@/lib/gmx-direct-trade";
import {
  getAnonymousUserId,
  isScreenerPinned,
  pinScreener,
  screenerId,
  unpinScreener,
} from "@/lib/local-store";
import type { ScreenerResult, ScreenerRow } from "@/lib/types";
import { cn } from "@/lib/utils";

type OrderTicket = {
  side: "Long" | "Short";
  row: ScreenerRow;
};

type SortKey = "rank" | "market" | "gmx" | `metric:${string}`;

type SortState = {
  key: SortKey;
  direction: "asc" | "desc";
};

type ScreenerResultCardProps = {
  result: ScreenerResult;
  onPinnedScreenersChange?: () => void;
};

export function ScreenerResultCard({
  result,
  onPinnedScreenersChange,
}: ScreenerResultCardProps) {
  const [current, setCurrent] = useState(result);
  const [isPinned, setIsPinned] = useState(() =>
    isScreenerPinned(result.definition),
  );
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [ticket, setTicket] = useState<OrderTicket | null>(null);
  const [isPinDialogOpen, setIsPinDialogOpen] = useState(false);
  const [pinLabel, setPinLabel] = useState(result.title);
  const [sort, setSort] = useState<SortState>({ key: "rank", direction: "asc" });
  const metricColumns = metricColumnsFor(current.rows);
  const sortedRows = [...current.rows].sort((left, right) => compareRows(left, right, sort));

  useEffect(() => {
    let isActive = true;
    async function loadPinnedState() {
      const userId = getAnonymousUserId();
      if (!userId) return;
      try {
        const response = await fetch(
          `/api/screeners/pins?user_id=${encodeURIComponent(userId)}`,
          { cache: "no-store" },
        );
        const payload = (await response.json().catch(() => null)) as unknown;
        if (!response.ok || !Array.isArray(payload)) return;
        const id = screenerId(result.definition);
        if (isActive) {
          setIsPinned(
            payload.some(
              (entry) =>
                entry &&
                typeof entry === "object" &&
                (entry as { id?: unknown }).id === id,
            ),
          );
        }
      } catch {
        // Local fallback state remains usable if the API is unavailable.
      }
    }
    void loadPinnedState();
    return () => {
      isActive = false;
    };
  }, [result.definition]);

  async function refresh() {
    setIsRefreshing(true);
    setRefreshError(null);
    try {
      const response = await fetch("/api/screeners/markets", {
        method: "POST",
        headers: { "content-type": "application/json" },
        cache: "no-store",
        body: JSON.stringify({
          factor: current.definition.factor,
          limit: current.definition.limit,
          gmxOnly: current.definition.gmx_only,
          asOf: current.definition.as_of,
        }),
      });
      const payload = (await response.json().catch(() => null)) as unknown;
      if (!response.ok || !isScreenerResult(payload)) {
        throw new Error("Unable to refresh screener");
      }
      setCurrent(payload);
      if (isPinned) {
        pinScreener(payload);
        onPinnedScreenersChange?.();
      }
    } catch (error) {
      setRefreshError(
        error instanceof Error ? error.message : "Unable to refresh screener",
      );
    } finally {
      setIsRefreshing(false);
    }
  }

  function togglePin() {
    const userId = getAnonymousUserId();
    if (isPinned) {
      if (userId) {
        void fetch(
          `/api/screeners/pins/${encodeURIComponent(screenerId(current.definition))}?user_id=${encodeURIComponent(userId)}`,
          { method: "DELETE" },
        ).catch(() => null);
      }
      unpinScreener(current.definition);
      setIsPinned(false);
      onPinnedScreenersChange?.();
      return;
    }
    setPinLabel(current.title);
    setIsPinDialogOpen(true);
  }

  function confirmPin() {
    const userId = getAnonymousUserId();
    const pinnedLabel = pinLabel.trim() || current.title;
    if (userId) {
      void fetch("/api/screeners/pins", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          user_id: userId,
          title: pinnedLabel,
          definition: current.definition,
        }),
      }).catch(() => null);
    }
    pinScreener(current, pinnedLabel);
    setIsPinned(true);
    setIsPinDialogOpen(false);
    onPinnedScreenersChange?.();
  }

  function toggleSort(key: SortKey) {
    setSort((currentSort) => {
      if (currentSort.key !== key) return { key, direction: "desc" };
      return {
        key,
        direction: currentSort.direction === "asc" ? "desc" : "asc",
      };
    });
  }

  return (
    <>
      <Card className="mx-auto w-full max-w-6xl border-primary/20 bg-[linear-gradient(135deg,color-mix(in_oklab,var(--primary)_8%,transparent),transparent_35%),var(--card)] shadow-sm">
        <CardHeader>
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="secondary">GMX screener</Badge>
            {current.definition.gmx_only ? (
              <Badge variant="outline">Tradeable only</Badge>
            ) : (
              <Badge variant="outline">Research + tradeable</Badge>
            )}
          </div>
          <CardTitle>{current.title}</CardTitle>
          <CardDescription>{current.summary}</CardDescription>
          <CardAction className="flex gap-2">
            <Button variant="outline" size="sm" onClick={togglePin}>
              <Pin className={cn("size-3.5", isPinned && "fill-current")} />
              {isPinned ? "Pinned" : "Pin"}
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={isRefreshing}
              onClick={refresh}
            >
              <RefreshCw
                className={cn("size-3.5", isRefreshing && "animate-spin")}
              />
              Refresh
            </Button>
          </CardAction>
        </CardHeader>
        <CardContent className="space-y-4">
          {refreshError ? (
            <p className="rounded-lg bg-destructive/10 px-3 py-2 text-sm text-destructive">
              {refreshError}
            </p>
          ) : null}
          <div className="overflow-x-auto rounded-xl border bg-background/70">
            <table className="w-full min-w-[920px] text-left text-sm">
              <thead className="border-b bg-muted/50 text-xs uppercase tracking-wide text-muted-foreground">
                <tr>
                  <SortableHeader
                    label="Rank"
                    sortKey="rank"
                    sort={sort}
                    onSort={toggleSort}
                  />
                  <SortableHeader
                    label="Market"
                    sortKey="market"
                    sort={sort}
                    onSort={toggleSort}
                  />
                  {metricColumns.map((metric) => (
                    <SortableHeader
                      key={metric.id}
                      label={metric.label}
                      sortKey={`metric:${metric.id}`}
                      sort={sort}
                      onSort={toggleSort}
                      align="right"
                    />
                  ))}
                  <SortableHeader
                    label="GMX"
                    sortKey="gmx"
                    sort={sort}
                    onSort={toggleSort}
                  />
                  <th className="px-3 py-2 text-right font-medium">Actions</th>
                </tr>
              </thead>
              <tbody>
                {sortedRows.map((row) => (
                  <tr key={row.coin_id} className="border-b last:border-0">
                    <td className="px-3 py-3 font-mono text-xs text-muted-foreground">
                      #{row.rank}
                    </td>
                    <td className="px-3 py-3">
                      <div className="font-semibold">{row.symbol}</div>
                      <div className="text-xs text-muted-foreground">
                        {row.market_name ?? row.coin_id}
                      </div>
                    </td>
                    {metricColumns.map((column) => {
                      const metric = row.metrics.find((item) => item.id === column.id);

                      return (
                        <td
                          key={column.id}
                          className="px-3 py-3 text-right font-mono text-sm font-medium tabular-nums"
                        >
                          {formatMetric(metric?.value ?? null, metric?.format ?? column.format)}
                        </td>
                      );
                    })}
                    <td className="px-3 py-3">
                      {row.is_gmx_tradeable ? (
                        <Badge variant="default">Arbitrum V2</Badge>
                      ) : (
                        <Badge variant="outline">Research only</Badge>
                      )}
                    </td>
                    <td className="px-3 py-3">
                      <div className="flex justify-end gap-2">
                        <Button
                          size="sm"
                          disabled={!row.actions.long.enabled}
                          onClick={() => setTicket({ side: "Long", row })}
                          className="border-emerald-500/30 bg-emerald-500/10 text-emerald-700 hover:bg-emerald-500/15 disabled:border-input disabled:bg-transparent disabled:text-muted-foreground dark:text-emerald-300"
                        >
                          <TrendingUp className="size-3.5" />
                          Long
                        </Button>
                        <Button
                          size="sm"
                          disabled={!row.actions.short.enabled}
                          onClick={() => setTicket({ side: "Short", row })}
                          className="border-red-500/30 bg-red-500/10 text-red-700 hover:bg-red-500/15 disabled:border-input disabled:bg-transparent disabled:text-muted-foreground dark:text-red-300"
                        >
                          <TrendingDown className="size-3.5" />
                          Short
                        </Button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="grid gap-2 text-xs leading-5 text-muted-foreground sm:grid-cols-2">
            {current.notes.map((note) => (
              <p key={note} className="rounded-lg border bg-muted/25 px-3 py-2">
                {note}
              </p>
            ))}
          </div>
        </CardContent>
      </Card>

      <Sheet open={Boolean(ticket)} onOpenChange={(open) => !open && setTicket(null)}>
        <SheetContent className="w-full sm:max-w-md">
          {ticket ? <OrderTicketPreview ticket={ticket} /> : null}
        </SheetContent>
      </Sheet>

      <Sheet open={isPinDialogOpen} onOpenChange={setIsPinDialogOpen}>
        <SheetContent
          side="bottom"
          className="!bottom-auto !left-1/2 !right-auto top-1/2 w-[calc(100%-2rem)] max-w-md -translate-x-1/2 -translate-y-1/2 rounded-2xl border p-0 shadow-2xl data-[side=bottom]:!bottom-auto data-[side=bottom]:!left-1/2 data-[side=bottom]:!right-auto data-[side=bottom]:border data-[side=bottom]:data-ending-style:translate-y-[-45%] data-[side=bottom]:data-starting-style:translate-y-[-45%]"
        >
          <form
            onSubmit={(event) => {
              event.preventDefault();
              confirmPin();
            }}
          >
            <SheetHeader className="border-b pr-14">
              <SheetTitle>Name this screener</SheetTitle>
              <SheetDescription>
                This name appears in the pinned screeners section of the sidebar.
              </SheetDescription>
            </SheetHeader>
            <div className="space-y-3 px-4 py-4">
              <label className="space-y-1.5 text-sm font-medium">
                <span>Screener name</span>
                <Input
                  autoFocus
                  value={pinLabel}
                  onChange={(event) => setPinLabel(event.target.value)}
                  placeholder="Momentum leaders"
                />
              </label>
              <p className="text-xs leading-5 text-muted-foreground">
                You can still refresh the screener and use the Long/Short actions
                after pinning it.
              </p>
            </div>
            <SheetFooter className="border-t sm:flex-row sm:justify-end">
              <Button
                type="button"
                variant="outline"
                onClick={() => setIsPinDialogOpen(false)}
              >
                Cancel
              </Button>
              <Button type="submit">Pin screener</Button>
            </SheetFooter>
          </form>
        </SheetContent>
      </Sheet>
    </>
  );
}

function OrderTicketPreview({ ticket }: { ticket: OrderTicket }) {
  const { authenticated, login } = usePrivy();
  const { wallets } = useWallets();
  const isShort = ticket.side === "Short";
  const [collateralUsd, setCollateralUsd] = useState("100");
  const [leverage, setLeverage] = useState("1");
  const [slippagePercent, setSlippagePercent] = useState("1");
  const [status, setStatus] = useState<"idle" | "approving" | "submitted">("idle");
  const [error, setError] = useState<string | null>(null);
  const [orderHash, setOrderHash] = useState<string | null>(null);

  async function submitOrder() {
    if (!authenticated) {
      login();
      return;
    }
    const wallet = wallets[0];
    if (!wallet) {
      setError("Connect a wallet before trading");
      return;
    }
    setStatus("approving");
    setError(null);
    setOrderHash(null);
    try {
      const provider = await wallet.getEthereumProvider();
      const result = await createDirectGmxMarketIncreaseOrder(provider, {
        row: ticket.row,
        side: ticket.side,
        collateralUsd,
        leverage,
        slippageBps: Math.round(Number(slippagePercent) * 100),
      });
      setOrderHash(result.orderHash);
      setStatus("submitted");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "GMX order failed");
      setStatus("idle");
    }
  }

  return (
    <>
      <SheetHeader>
        <SheetTitle>
          {ticket.side} {ticket.row.symbol} on GMX
        </SheetTitle>
        <SheetDescription>
          Creates a GMX V2 market increase order directly from your connected
          wallet. You will sign a USDC approval, then the GMX order transaction.
        </SheetDescription>
      </SheetHeader>
      <div className="space-y-4 px-4">
        <div className="grid gap-2 rounded-xl border bg-muted/25 p-3 text-sm">
          <TicketLine label="Side" value={ticket.side} />
          <TicketLine label="Market" value={ticket.row.market_name ?? ticket.row.symbol} />
          <TicketLine label="Collateral" value="USDC on Arbitrum" />
          <TicketLine label="Execution" value="GMX market increase" />
        </div>
        <div className="grid gap-3 rounded-xl border p-3 text-sm">
          <label className="space-y-1.5 font-medium">
            <span>Collateral amount (USDC)</span>
            <Input
              inputMode="decimal"
              value={collateralUsd}
              onChange={(event) => setCollateralUsd(event.target.value)}
              placeholder="100"
            />
          </label>
          <label className="space-y-1.5 font-medium">
            <span>Leverage</span>
            <Input
              inputMode="decimal"
              value={leverage}
              onChange={(event) => setLeverage(event.target.value)}
              placeholder="1"
            />
          </label>
          <label className="space-y-1.5 font-medium">
            <span>Slippage tolerance (%)</span>
            <Input
              inputMode="decimal"
              value={slippagePercent}
              onChange={(event) => setSlippagePercent(event.target.value)}
              placeholder="1"
            />
          </label>
        </div>
        <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-sm leading-6 text-amber-900 dark:text-amber-100">
          {isShort
            ? "Shorts require explicit confirmation because losses can grow as the market rises."
            : "Leveraged longs above 1.0x require explicit confirmation before signing."}
        </div>
        <p className="text-xs leading-5 text-muted-foreground">
          This does not use a StrategyVault. It approves the GMX Router to pull
          your USDC and submits `ExchangeRouter.multicall` from your wallet.
          Execution fee is estimated from live Arbitrum RPC. Acceptable price is
          computed from live GMX ticker prices and your slippage tolerance.
        </p>
        {error ? <p className="text-sm text-destructive">{error}</p> : null}
        {orderHash ? (
          <p className="break-all text-xs text-muted-foreground">
            Submitted: {orderHash}
          </p>
        ) : null}
      </div>
      <SheetFooter>
        {ticket.row.gmx_market ? (
          <Button
            onClick={submitOrder}
            disabled={status === "approving" || status === "submitted"}
          >
            {status === "approving"
              ? "Signing GMX order..."
              : status === "submitted"
                ? "GMX order submitted"
                : authenticated
                  ? `Trade ${ticket.side} on GMX`
                  : "Log in to trade"}
          </Button>
        ) : (
          <Button disabled>GMX market unavailable</Button>
        )}
      </SheetFooter>
    </>
  );
}

function TicketLine({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-4">
      <span className="text-muted-foreground">{label}</span>
      <span className="text-right font-medium">{value}</span>
    </div>
  );
}

function SortableHeader({
  label,
  sortKey,
  sort,
  onSort,
  align = "left",
}: {
  label: string;
  sortKey: SortKey;
  sort: SortState;
  onSort: (key: SortKey) => void;
  align?: "left" | "right";
}) {
  const isActive = sort.key === sortKey;
  const directionLabel = sort.direction === "asc" ? "ascending" : "descending";

  return (
    <th
      className={cn(
        "px-3 py-2 font-medium",
        align === "right" && "text-right",
      )}
      aria-sort={isActive ? directionLabel : "none"}
    >
      <button
        type="button"
        onClick={() => onSort(sortKey)}
        className={cn(
          "inline-flex items-center gap-1.5 rounded-md transition-colors hover:text-foreground",
          align === "right" && "justify-end",
        )}
      >
        <span>{label}</span>
        <ArrowUpDown
          className={cn("size-3", isActive ? "text-foreground" : "opacity-50")}
        />
      </button>
    </th>
  );
}

function metricColumnsFor(rows: ScreenerRow[]) {
  const columns: { id: string; label: string; format: "percent" | "number" }[] = [];
  const seen = new Set<string>();

  for (const row of rows) {
    for (const metric of row.metrics) {
      if (seen.has(metric.id)) continue;
      seen.add(metric.id);
      columns.push({
        id: metric.id,
        label: metric.label,
        format: metric.format,
      });
    }
  }

  return columns;
}

function compareRows(left: ScreenerRow, right: ScreenerRow, sort: SortState) {
  const multiplier = sort.direction === "asc" ? 1 : -1;
  return compareSortValues(sortValue(left, sort.key), sortValue(right, sort.key)) * multiplier;
}

function sortValue(row: ScreenerRow, key: SortKey) {
  if (key === "rank") return row.rank;
  if (key === "market") return row.symbol || row.market_name || row.coin_id;
  if (key === "gmx") return row.is_gmx_tradeable ? 1 : 0;

  const metricId = key.slice("metric:".length);
  return row.metrics.find((metric) => metric.id === metricId)?.value ?? null;
}

function compareSortValues(left: string | number | null, right: string | number | null) {
  if (left === null && right === null) return 0;
  if (left === null) return 1;
  if (right === null) return -1;

  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }

  return String(left).localeCompare(String(right), undefined, {
    numeric: true,
    sensitivity: "base",
  });
}

function formatMetric(value: number | null, format: "percent" | "number") {
  if (value === null || !Number.isFinite(value)) return "n/a";
  if (format === "percent") return `${(value * 100).toFixed(1)}%`;
  return value.toFixed(2);
}

function isScreenerResult(value: unknown): value is ScreenerResult {
  return (
    Boolean(value) &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    (value as { type?: unknown }).type === "market_screener" &&
    Array.isArray((value as { rows?: unknown }).rows)
  );
}
