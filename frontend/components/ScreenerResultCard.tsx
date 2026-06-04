"use client";

import { useEffect, useState } from "react";
import { Pin, RefreshCw, TrendingDown, TrendingUp } from "lucide-react";

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

type ScreenerResultCardProps = {
  result: ScreenerResult;
};

export function ScreenerResultCard({ result }: ScreenerResultCardProps) {
  const [current, setCurrent] = useState(result);
  const [isPinned, setIsPinned] = useState(() =>
    isScreenerPinned(result.definition),
  );
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [ticket, setTicket] = useState<OrderTicket | null>(null);

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
      const userId = getAnonymousUserId();
      const response = await fetch(
        isPinned && userId
          ? `/api/screeners/pins/${encodeURIComponent(screenerId(current.definition))}/refresh`
          : "/api/screeners/markets",
        {
        method: "POST",
        headers: { "content-type": "application/json" },
        cache: "no-store",
        body: JSON.stringify(
          isPinned && userId
            ? { user_id: userId }
            : {
                factor: current.definition.factor,
                limit: current.definition.limit,
                gmxOnly: current.definition.gmx_only,
                asOf: current.definition.as_of,
              },
        ),
      },
      );
      const payload = (await response.json().catch(() => null)) as unknown;
      if (!response.ok || !isScreenerResult(payload)) {
        throw new Error("Unable to refresh screener");
      }
      setCurrent(payload);
      if (isPinned) pinScreener(payload);
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
      return;
    }
    if (userId) {
      void fetch("/api/screeners/pins", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          user_id: userId,
          title: current.title,
          definition: current.definition,
        }),
      }).catch(() => null);
    }
    pinScreener(current);
    setIsPinned(true);
  }

  return (
    <>
      <Card className="max-w-6xl border-primary/20 bg-[linear-gradient(135deg,color-mix(in_oklab,var(--primary)_8%,transparent),transparent_35%),var(--card)] shadow-sm">
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
            <table className="w-full min-w-[760px] text-left text-sm">
              <thead className="border-b bg-muted/50 text-xs uppercase tracking-wide text-muted-foreground">
                <tr>
                  <th className="px-3 py-2 font-medium">Rank</th>
                  <th className="px-3 py-2 font-medium">Market</th>
                  <th className="px-3 py-2 font-medium">Metrics</th>
                  <th className="px-3 py-2 font-medium">GMX</th>
                  <th className="px-3 py-2 text-right font-medium">Actions</th>
                </tr>
              </thead>
              <tbody>
                {current.rows.map((row) => (
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
                    <td className="px-3 py-3">
                      <div className="flex flex-wrap gap-2">
                        {row.metrics.map((metric) => (
                          <span
                            key={metric.id}
                            className="rounded-full border bg-muted/35 px-2.5 py-1 text-xs"
                          >
                            <span className="text-muted-foreground">
                              {metric.label}: 
                            </span>
                            <span className="font-mono font-medium">
                              {formatMetric(metric.value, metric.format)}
                            </span>
                          </span>
                        ))}
                      </div>
                    </td>
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
                        >
                          <TrendingUp className="size-3.5" />
                          Long
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={!row.actions.short.enabled}
                          onClick={() => setTicket({ side: "Short", row })}
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
    </>
  );
}

function OrderTicketPreview({ ticket }: { ticket: OrderTicket }) {
  const isShort = ticket.side === "Short";
  return (
    <>
      <SheetHeader>
        <SheetTitle>
          {ticket.side} {ticket.row.symbol} on GMX
        </SheetTitle>
        <SheetDescription>
          Confirmation preview only. The next slice will connect this ticket to
          the user&apos;s Privy wallet and GMX order router.
        </SheetDescription>
      </SheetHeader>
      <div className="space-y-4 px-4">
        <div className="grid gap-2 rounded-xl border bg-muted/25 p-3 text-sm">
          <TicketLine label="Side" value={ticket.side} />
          <TicketLine label="Market" value={ticket.row.market_name ?? ticket.row.symbol} />
          <TicketLine label="Collateral" value="USDC on Arbitrum" />
          <TicketLine label="Notional" value="User enters before signing" />
          <TicketLine label="Leverage" value="1.0x default" />
          <TicketLine label="Slippage" value="User confirms acceptable price" />
          <TicketLine label="Execution fee" value="Estimated before signing" />
        </div>
        <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-sm leading-6 text-amber-900 dark:text-amber-100">
          {isShort
            ? "Shorts require explicit confirmation because losses can grow as the market rises."
            : "Leveraged longs above 1.0x require explicit confirmation before signing."}
        </div>
        <p className="text-xs leading-5 text-muted-foreground">
          uiFeeReceiver will be read from configuration when transaction signing
          is wired. No StrategyVault is used for these discretionary orders.
        </p>
      </div>
      <SheetFooter>
        <Button disabled>Connect GMX signing in next slice</Button>
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
