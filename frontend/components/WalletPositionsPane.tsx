"use client";

import { useEffect, useState, type ReactNode } from "react";
import { ArrowLeft, RefreshCw, WalletCards } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  readGmxAccountActivity,
  type GmxAccountActivity,
  type GmxOpenPosition,
  type GmxPendingOrder,
} from "@/lib/gmx-positions";
import { cn } from "@/lib/utils";

type WalletPositionsPaneProps = {
  walletAddress: string | null;
  onBack: () => void;
  onActivityCountChange?: (count: number) => void;
};

const EMPTY_ACTIVITY: GmxAccountActivity = {
  openPositions: [],
  pendingOrders: [],
};

const usdFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 2,
});

const tokenFormatter = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 6,
});

export function WalletPositionsPane({
  walletAddress,
  onBack,
  onActivityCountChange,
}: WalletPositionsPaneProps) {
  const [activity, setActivity] = useState<GmxAccountActivity>(EMPTY_ACTIVITY);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function refresh() {
    if (!walletAddress) {
      setActivity(EMPTY_ACTIVITY);
      onActivityCountChange?.(0);
      return;
    }

    setIsLoading(true);
    setError(null);
    try {
      const next = await readGmxAccountActivity([
        { type: "wallet", address: walletAddress },
      ]);
      setActivity(next);
      onActivityCountChange?.(next.openPositions.length + next.pendingOrders.length);
    } catch (refreshError) {
      setError(
        refreshError instanceof Error
          ? refreshError.message
          : "Unable to load wallet positions",
      );
    } finally {
      setIsLoading(false);
    }
  }

  useEffect(() => {
    let isActive = true;

    async function load() {
      if (!walletAddress) {
        setActivity(EMPTY_ACTIVITY);
        onActivityCountChange?.(0);
        return;
      }

      setIsLoading(true);
      setError(null);
      try {
        const next = await readGmxAccountActivity([
          { type: "wallet", address: walletAddress },
        ]);
        if (!isActive) return;
        setActivity(next);
        onActivityCountChange?.(next.openPositions.length + next.pendingOrders.length);
      } catch (loadError) {
        if (!isActive) return;
        setError(
          loadError instanceof Error
            ? loadError.message
            : "Unable to load wallet positions",
        );
      } finally {
        if (isActive) setIsLoading(false);
      }
    }

    void load();
    return () => {
      isActive = false;
    };
  }, [walletAddress, onActivityCountChange]);

  const total = activity.openPositions.length + activity.pendingOrders.length;

  return (
    <div className="min-h-0 flex-1 overflow-y-auto bg-background">
      <div className="sticky top-0 z-10 flex items-center justify-between gap-3 border-b border-border/60 bg-background/95 px-4 py-3 backdrop-blur">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.3em] text-muted-foreground">
            Direct positions
          </p>
          <h1 className="font-heading text-lg font-semibold">
            Direct GMX activity
          </h1>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => void refresh()}
            disabled={isLoading || !walletAddress}
          >
            <RefreshCw className={cn("size-3.5", isLoading && "animate-spin")} />
            Refresh
          </Button>
          <Button variant="outline" size="sm" onClick={onBack}>
            <ArrowLeft className="size-3.5" />
            Back to chat
          </Button>
        </div>
      </div>
      <div className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-8">
        <Card className="border-primary/15 bg-[linear-gradient(135deg,color-mix(in_oklab,var(--primary)_7%,transparent),transparent_35%),var(--card)]">
          <CardHeader className="flex flex-row items-start justify-between gap-3">
            <div className="space-y-1">
              <CardTitle className="flex items-center gap-2">
                <WalletCards className="size-4 text-primary" />
                Direct GMX positions
              </CardTitle>
              <p className="text-sm text-muted-foreground">
                Positions opened directly from screener Long/Short actions.
                Vault-managed activity stays under Deployed strategies.
              </p>
            </div>
            <Badge variant={total > 0 ? "default" : "outline"}>{total}</Badge>
          </CardHeader>
          <CardContent className="space-y-5">
            {!walletAddress ? (
              <EmptyState title="Connect a wallet" description="Log in with a wallet to load direct GMX positions." />
            ) : error ? (
              <p className="rounded-lg bg-destructive/10 px-3 py-2 text-sm text-destructive">
                {error}
              </p>
            ) : isLoading && total === 0 ? (
              <p className="rounded-lg border bg-muted/30 px-3 py-6 text-center text-sm text-muted-foreground">
                Loading wallet positions...
              </p>
            ) : total === 0 ? (
              <EmptyState
                title="No direct positions yet"
                description="Direct Long/Short trades from screeners will appear here. Vault-managed positions are intentionally excluded."
              />
            ) : (
              <>
                <PositionsTable rows={activity.openPositions} />
                <OrdersTable rows={activity.pendingOrders} />
              </>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function PositionsTable({ rows }: { rows: GmxOpenPosition[] }) {
  return (
    <ActivityTable title="Open" empty="No open direct positions">
      {rows.map((row) => (
        <tr key={row.id} className="border-b last:border-0">
          <td className="px-3 py-3 font-medium">{row.marketName}</td>
          <td className="px-3 py-3">
            <SideBadge side={row.side} />
          </td>
          <td className="px-3 py-3 text-right font-mono tabular-nums">
            {usdFormatter.format(row.sizeUsd)}
          </td>
          <td className="px-3 py-3 text-right font-mono tabular-nums">
            {tokenFormatter.format(row.collateralAmount)} {row.collateralSymbol}
          </td>
          <td className="px-3 py-3 text-right text-xs text-muted-foreground">
            {formatUpdatedAt(row.updatedAt)}
          </td>
        </tr>
      ))}
    </ActivityTable>
  );
}

function OrdersTable({ rows }: { rows: GmxPendingOrder[] }) {
  return (
    <ActivityTable title="Pending orders" empty="No pending direct orders">
      {rows.map((row) => (
        <tr key={row.id} className="border-b last:border-0">
          <td className="px-3 py-3 font-medium">{row.marketName}</td>
          <td className="px-3 py-3">
            <SideBadge side={row.side} />
          </td>
          <td className="px-3 py-3 text-sm text-muted-foreground">
            {row.orderType}
          </td>
          <td className="px-3 py-3 text-right font-mono tabular-nums">
            {usdFormatter.format(row.sizeUsd)}
          </td>
          <td className="px-3 py-3 text-right font-mono tabular-nums">
            {tokenFormatter.format(row.collateralAmount)} {row.collateralSymbol}
          </td>
          <td className="px-3 py-3 text-right text-xs text-muted-foreground">
            {formatUpdatedAt(row.updatedAt)}
          </td>
        </tr>
      ))}
    </ActivityTable>
  );
}

function ActivityTable({
  title,
  empty,
  children,
}: {
  title: string;
  empty: string;
  children: ReactNode;
}) {
  const hasRows = Array.isArray(children) ? children.length > 0 : Boolean(children);

  return (
    <section className="space-y-2">
      <div className="flex items-center justify-between gap-2">
        <h2 className="text-sm font-semibold">{title}</h2>
      </div>
      <div className="overflow-x-auto rounded-xl border bg-background/70">
        <table className="w-full min-w-[760px] text-left text-sm">
          <thead className="border-b bg-muted/50 text-xs uppercase tracking-wide text-muted-foreground">
            <tr>
              <th className="px-3 py-2 font-medium">Market</th>
              <th className="px-3 py-2 font-medium">Side</th>
              {title === "Pending orders" ? (
                <th className="px-3 py-2 font-medium">Type</th>
              ) : null}
              <th className="px-3 py-2 text-right font-medium">Size</th>
              <th className="px-3 py-2 text-right font-medium">Collateral</th>
              <th className="px-3 py-2 text-right font-medium">Updated</th>
            </tr>
          </thead>
          <tbody>
            {hasRows ? (
              children
            ) : (
              <tr>
                <td
                  className="px-3 py-5 text-center text-sm text-muted-foreground"
                  colSpan={title === "Pending orders" ? 6 : 5}
                >
                  {empty}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function SideBadge({ side }: { side: "Long" | "Short" }) {
  return (
    <Badge
      variant="outline"
      className={cn(
        side === "Long"
          ? "border-emerald-500/30 text-emerald-700 dark:text-emerald-300"
          : "border-red-500/30 text-red-700 dark:text-red-300",
      )}
    >
      {side}
    </Badge>
  );
}

function EmptyState({ title, description }: { title: string; description: string }) {
  return (
    <div className="rounded-xl border border-dashed bg-muted/20 px-4 py-10 text-center">
      <p className="font-medium">{title}</p>
      <p className="mx-auto mt-2 max-w-md text-sm text-muted-foreground">
        {description}
      </p>
    </div>
  );
}

function formatUpdatedAt(value?: string) {
  if (!value) return "-";
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}
