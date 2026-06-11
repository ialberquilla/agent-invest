"use client";

import { startTransition, useCallback, useEffect, useState } from "react";
import { usePrivy, useWallets } from "@privy-io/react-auth";

import { ChatView } from "@/components/ChatView";
import { ScreenerResultCard } from "@/components/ScreenerResultCard";
import { StrategySidebar } from "@/components/StrategySidebar";
import { ThemeToggle } from "@/components/ThemeToggle";
import { VaultManagerPane } from "@/components/VaultManagerPane";
import { WalletPositionsPane } from "@/components/WalletPositionsPane";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  clearStrategyId,
  ensureKnownStrategy,
  getKnownStrategies,
  getDeployedStrategies,
  getAnonymousUserId,
  getPinnedScreeners,
  getStrategyId,
  removeKnownStrategy,
  type DeployedStrategy,
  type PinnedScreener,
  setStrategyId as persistStrategyId,
} from "@/lib/local-store";
import { readGmxAccountActivity } from "@/lib/gmx-positions";
import type { ScreenerResult, StrategyCreateResponse } from "@/lib/types";

type AuthState = {
  ready: boolean;
  authenticated: boolean;
  user?: {
    id?: string;
    email?: { address?: string | null } | null;
    wallet?: { address?: string | null } | null;
  } | null;
  walletAddress?: string | null;
  login: () => void;
  logout: () => void;
  getAccessToken: () => Promise<string | null>;
};

type DeployedStrategyStatus = {
  label: string;
  tone: "idle" | "pending" | "live" | "error";
};

const anonymousAuth: AuthState = {
  ready: true,
  authenticated: false,
  user: null,
  login: () => undefined,
  logout: () => undefined,
  getAccessToken: async () => null,
  walletAddress: null,
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

async function readJson(response: Response) {
  try {
    return (await response.json()) as unknown;
  } catch {
    return null;
  }
}

function getErrorMessage(payload: unknown) {
  if (
    isRecord(payload) &&
    typeof payload.message === "string" &&
    payload.message.trim()
  ) {
    return payload.message.trim();
  }

  return "Unable to create a strategy";
}

function shortenIdentity(value: string) {
  if (value.includes("@")) return value;
  if (value.length <= 14) return value;

  return `${value.slice(0, 6)}...${value.slice(-4)}`;
}

async function authHeaders(
  authenticated: boolean,
  getAccessToken: () => Promise<string | null>,
) {
  if (!authenticated) return undefined;

  const token = await getAccessToken();
  return token ? { authorization: `Bearer ${token}` } : undefined;
}

async function requestStrategy(
  authenticated: boolean,
  getAccessToken: () => Promise<string | null>,
) {
  const anonymousUserId = getAnonymousUserId();
  const response = await fetch("/api/strategies", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(await authHeaders(authenticated, getAccessToken)),
    },
    cache: "no-store",
    body: JSON.stringify({ user_id: anonymousUserId }),
  });
  const payload = await readJson(response);

  if (!response.ok) {
    throw new Error(getErrorMessage(payload));
  }

  if (!isRecord(payload) || typeof payload.strategy_id !== "string") {
    throw new Error("Strategy creation returned an invalid response");
  }

  return payload as StrategyCreateResponse;
}

export function StrategyChatShell() {
  if (!process.env.NEXT_PUBLIC_PRIVY_APP_ID) {
    return <StrategyChatShellContent auth={anonymousAuth} />;
  }

  return <PrivyStrategyChatShell />;
}

function PrivyStrategyChatShell() {
  const { ready, authenticated, user, login, logout, getAccessToken } =
    usePrivy();
  const { wallets } = useWallets();
  const walletAddress = wallets[0]?.address ?? user?.wallet?.address ?? null;

  return (
    <StrategyChatShellContent
      auth={{
        ready,
        authenticated,
        user,
        walletAddress,
        login,
        logout,
        getAccessToken,
      }}
    />
  );
}

function StrategyChatShellContent({ auth }: { auth: AuthState }) {
  const { ready, authenticated, user, login, logout, getAccessToken } = auth;
  const identity = user?.email?.address ?? user?.wallet?.address ?? user?.id;
  const identityLabel = identity ? shortenIdentity(identity) : "Connected";
  const walletAddress = authenticated
    ? auth.walletAddress?.trim() || user?.wallet?.address?.trim() || null
    : null;
  const [strategyId, setStrategyId] = useState<string | null>(null);
  const [knownStrategies, setKnownStrategies] = useState(() =>
    getKnownStrategies(),
  );
  const [pinnedScreeners, setPinnedScreeners] = useState(() =>
    getPinnedScreeners(),
  );
  const [deployedStrategies, setDeployedStrategies] = useState(() =>
    getDeployedStrategies(),
  );
  const [deployedStrategyStatuses, setDeployedStrategyStatuses] = useState<
    Record<string, DeployedStrategyStatus>
  >({});
  const [activeScreenerId, setActiveScreenerId] = useState<string | null>(null);
  const [activeScreener, setActiveScreener] = useState<ScreenerResult | null>(
    null,
  );
  const [selectedVault, setSelectedVault] = useState<DeployedStrategy | null>(
    null,
  );
  const [isWalletPositionsActive, setIsWalletPositionsActive] = useState(false);
  const [walletPositionCount, setWalletPositionCount] = useState<number | null>(
    null,
  );
  const [walletPositionStatus, setWalletPositionStatus] = useState<
    "idle" | "loading" | "error"
  >("idle");
  const [screenerError, setScreenerError] = useState<string | null>(null);
  const [isLoadingScreener, setIsLoadingScreener] = useState(false);
  const [bootstrapError, setBootstrapError] = useState<string | null>(null);
  const [strategyError, setStrategyError] = useState<string | null>(null);
  const [bootstrapKey, setBootstrapKey] = useState(0);
  const [isCreatingStrategy, setIsCreatingStrategy] = useState(false);
  const [isChatBusy, setIsChatBusy] = useState(false);

  function refreshKnownStrategies() {
    setKnownStrategies(getKnownStrategies());
  }

  function refreshPinnedScreeners() {
    setPinnedScreeners(getPinnedScreeners());
  }

  function refreshDeployedStrategies() {
    setDeployedStrategies(getDeployedStrategies());
  }

  const handleWalletActivityCountChange = useCallback((count: number) => {
    setWalletPositionCount(count);
    setWalletPositionStatus("idle");
  }, []);

  useEffect(() => {
    window.addEventListener(
      "agent-invest:deployed-strategies",
      refreshDeployedStrategies,
    );
    return () => {
      window.removeEventListener(
        "agent-invest:deployed-strategies",
        refreshDeployedStrategies,
      );
    };
  }, []);

  useEffect(() => {
    let isActive = true;

    async function loadDeployedStrategyStatuses() {
      if (deployedStrategies.length === 0) {
        setDeployedStrategyStatuses({});
        return;
      }

      try {
        const activity = await readGmxAccountActivity(
          deployedStrategies.map((strategy) => ({
            type: "vault" as const,
            address: strategy.vault_address,
            label: strategy.label,
          })),
        );
        if (!isActive) return;

        const next: Record<string, DeployedStrategyStatus> = {};
        for (const strategy of deployedStrategies) {
          const address = strategy.vault_address.toLowerCase();
          const openCount = activity.openPositions.filter(
            (position) => position.accountAddress.toLowerCase() === address,
          ).length;
          const pendingCount = activity.pendingOrders.filter(
            (order) => order.accountAddress.toLowerCase() === address,
          ).length;

          if (pendingCount > 0) {
            next[strategy.mandate_id] = {
              label: pendingCount === 1 ? "pending" : `${pendingCount} pending`,
              tone: "pending",
            };
          } else if (openCount > 0) {
            next[strategy.mandate_id] = {
              label: openCount === 1 ? "live" : `${openCount} live`,
              tone: "live",
            };
          } else {
            next[strategy.mandate_id] = {
              label: strategy.status,
              tone: "idle",
            };
          }
        }
        setDeployedStrategyStatuses(next);
      } catch {
        if (!isActive) return;
        setDeployedStrategyStatuses(
          Object.fromEntries(
            deployedStrategies.map((strategy) => [
              strategy.mandate_id,
              {
                label: "unknown",
                tone: "error" satisfies DeployedStrategyStatus["tone"],
              },
            ]),
          ),
        );
      }
    }

    void loadDeployedStrategyStatuses();
    const interval = window.setInterval(
      () => void loadDeployedStrategyStatuses(),
      30_000,
    );
    return () => {
      isActive = false;
      window.clearInterval(interval);
    };
  }, [deployedStrategies]);

  useEffect(() => {
    let isActive = true;

    async function loadWalletActivityCount() {
      if (!walletAddress) {
        setWalletPositionCount(null);
        setWalletPositionStatus("idle");
        return;
      }

      setWalletPositionStatus("loading");
      try {
        const activity = await readGmxAccountActivity([
          { type: "wallet", address: walletAddress },
        ]);
        if (!isActive) return;
        setWalletPositionCount(
          activity.openPositions.length + activity.pendingOrders.length,
        );
        setWalletPositionStatus("idle");
      } catch {
        if (!isActive) return;
        setWalletPositionCount(null);
        setWalletPositionStatus("error");
      }
    }

    void loadWalletActivityCount();
    return () => {
      isActive = false;
    };
  }, [walletAddress]);

  useEffect(() => {
    if (!ready || !authenticated) return;

    let isActive = true;
    async function claimAnonymousStrategies() {
      const anonymousUserId = getAnonymousUserId();
      if (!anonymousUserId) return;
      await fetch("/api/users/claim", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...(await authHeaders(authenticated, getAccessToken)),
        },
        body: JSON.stringify({ user_id: anonymousUserId }),
      }).catch(() => null);
      if (isActive) setBootstrapKey((current) => current + 1);
    }

    void claimAnonymousStrategies();
    return () => {
      isActive = false;
    };
  }, [ready, authenticated, getAccessToken]);

  useEffect(() => {
    let isActive = true;

    async function bootstrap() {
      setBootstrapError(null);
      setStrategyError(null);

      if (!ready) return;

      const cachedStrategyId = getStrategyId();
      if (cachedStrategyId) {
        ensureKnownStrategy(cachedStrategyId);

        if (isActive) {
          refreshKnownStrategies();
          setStrategyId(cachedStrategyId);
        }
        return;
      }

      try {
        const next = await requestStrategy(authenticated, getAccessToken);

        if (!isActive) {
          return;
        }

        ensureKnownStrategy(next.strategy_id);
        persistStrategyId(next.strategy_id);
        refreshKnownStrategies();
        setStrategyId(next.strategy_id);
      } catch (error) {
        if (!isActive) {
          return;
        }

        clearStrategyId();
        setBootstrapError(
          error instanceof Error
            ? error.message
            : "Unable to create a strategy",
        );
      }
    }

    void bootstrap();

    return () => {
      isActive = false;
    };
  }, [bootstrapKey, ready, authenticated, getAccessToken]);

  async function handleNewStrategy() {
    if (isCreatingStrategy) {
      return;
    }

    setStrategyError(null);
    setIsWalletPositionsActive(false);
    setSelectedVault(null);
    setIsCreatingStrategy(true);

    try {
      const next = await requestStrategy(authenticated, getAccessToken);

      ensureKnownStrategy(next.strategy_id);
      persistStrategyId(next.strategy_id);
      refreshKnownStrategies();
      startTransition(() => {
        setStrategyId(next.strategy_id);
      });
    } catch (error) {
      setStrategyError(
        error instanceof Error
          ? error.message
          : "Unable to create a new strategy",
      );
    } finally {
      setIsCreatingStrategy(false);
    }
  }

  function handleSelectStrategy(nextStrategyId: string) {
    if (isCreatingStrategy || nextStrategyId === strategyId) {
      return;
    }

    setStrategyError(null);
    setIsWalletPositionsActive(false);
    setSelectedVault(null);
    setActiveScreenerId(null);
    setActiveScreener(null);
    persistStrategyId(nextStrategyId);
    startTransition(() => {
      setStrategyId(nextStrategyId);
    });
  }

  async function handleDeleteStrategy(deletedStrategyId: string) {
    if (isCreatingStrategy || isChatBusy) {
      return;
    }

    const remainingStrategies = removeKnownStrategy(deletedStrategyId);
    setKnownStrategies(remainingStrategies);

    if (deletedStrategyId !== strategyId) {
      return;
    }

    const nextStrategyId = remainingStrategies[0]?.strategy_id;
    setStrategyError(null);
    setIsWalletPositionsActive(false);
    setSelectedVault(null);
    setActiveScreenerId(null);
    setActiveScreener(null);

    if (nextStrategyId) {
      persistStrategyId(nextStrategyId);
      startTransition(() => {
        setStrategyId(nextStrategyId);
      });
      return;
    }

    clearStrategyId();
    setStrategyId(null);
    setIsCreatingStrategy(true);

    try {
      const next = await requestStrategy(authenticated, getAccessToken);
      ensureKnownStrategy(next.strategy_id);
      persistStrategyId(next.strategy_id);
      refreshKnownStrategies();
      startTransition(() => {
        setStrategyId(next.strategy_id);
      });
    } catch (error) {
      setBootstrapError(
        error instanceof Error
          ? error.message
          : "Unable to create a replacement strategy",
      );
    } finally {
      setIsCreatingStrategy(false);
    }
  }

  async function handleSelectScreener(screener: PinnedScreener) {
    if (isCreatingStrategy || screener.id === activeScreenerId) {
      return;
    }

    setStrategyError(null);
    setScreenerError(null);
    setIsWalletPositionsActive(false);
    setSelectedVault(null);
    setActiveScreenerId(screener.id);
    setActiveScreener(null);
    setIsLoadingScreener(true);

    try {
      const response = await fetch("/api/screeners/markets", {
        method: "POST",
        headers: { "content-type": "application/json" },
        cache: "no-store",
        body: JSON.stringify({
          factor: screener.definition.factor,
          limit: screener.definition.limit,
          gmxOnly: screener.definition.gmx_only,
          asOf: screener.definition.as_of,
        }),
      });
      const payload = (await response.json().catch(() => null)) as unknown;
      if (!response.ok || !isScreenerResult(payload)) {
        throw new Error("Unable to load pinned screener");
      }
      setActiveScreener(payload);
    } catch (error) {
      setScreenerError(
        error instanceof Error
          ? error.message
          : "Unable to load pinned screener",
      );
    } finally {
      setIsLoadingScreener(false);
    }
  }

  function handleSelectWalletPositions() {
    if (isCreatingStrategy || isWalletPositionsActive) {
      return;
    }

    setStrategyError(null);
    setScreenerError(null);
    setActiveScreenerId(null);
    setActiveScreener(null);
    setSelectedVault(null);
    setIsWalletPositionsActive(true);
  }

  function handleSelectDeployedStrategy(strategy: DeployedStrategy) {
    if (isCreatingStrategy) return;
    setStrategyError(null);
    setScreenerError(null);
    setActiveScreenerId(null);
    setActiveScreener(null);
    setIsWalletPositionsActive(false);
    setSelectedVault(strategy);
  }

  if (bootstrapError) {
    return (
      <main className="flex min-h-screen items-center justify-center bg-muted/30 px-4 py-6 sm:px-6">
        <Card className="w-full max-w-md border-border/70 bg-background shadow-sm">
          <CardHeader>
            <CardTitle>Chat unavailable</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm text-destructive">{bootstrapError}</p>
            <Button
              variant="outline"
              onClick={() => setBootstrapKey((current) => current + 1)}
            >
              Retry
            </Button>
          </CardContent>
        </Card>
      </main>
    );
  }
  if (!ready || !strategyId) {
    return (
      <main className="flex min-h-screen items-center justify-center bg-muted/30 px-4 py-6 sm:px-6">
        <Card className="w-full max-w-md border-border/70 bg-background shadow-sm">
          <CardHeader>
            <CardTitle>Bootstrapping chat</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">
              Minting a strategy for the first conversation...
            </p>
          </CardContent>
        </Card>
      </main>
    );
  }

  return (
    <main className="flex h-dvh overflow-hidden bg-background text-foreground">
      <StrategySidebar
        strategies={knownStrategies}
        screeners={pinnedScreeners}
        deployedStrategies={deployedStrategies}
        deployedStrategyStatuses={deployedStrategyStatuses}
        activeStrategyId={strategyId}
        activeScreenerId={activeScreenerId}
        isWalletPositionsActive={isWalletPositionsActive}
        walletPositionCount={walletPositionCount}
        walletPositionStatus={walletPositionStatus}
        activeVaultMandateId={selectedVault?.mandate_id ?? null}
        disabled={isCreatingStrategy}
        deleteDisabled={isCreatingStrategy || isChatBusy}
        onSelectStrategy={handleSelectStrategy}
        onDeleteStrategy={handleDeleteStrategy}
        onSelectScreener={handleSelectScreener}
        onSelectWalletPositions={handleSelectWalletPositions}
        onSelectDeployedStrategy={handleSelectDeployedStrategy}
        onNewStrategy={handleNewStrategy}
      />

      <div className="flex min-w-0 flex-1 flex-col">
        <div className="flex items-center justify-end gap-2 border-b border-border/60 px-4 py-2 text-sm">
          {authenticated ? (
            <>
              <span
                className="inline-flex min-w-0 max-w-[13rem] items-center gap-2 rounded-full border border-border/70 bg-muted/45 px-3 py-1.5 text-muted-foreground shadow-xs sm:max-w-[18rem]"
                title={identity}
              >
                <span className="size-2 shrink-0 rounded-full bg-emerald-500" />
                <span className="truncate font-mono text-xs">
                  {identityLabel}
                </span>
              </span>
              <ThemeToggle compact surface="topbar" />
              <Button
                size="sm"
                className="rounded-full px-4 shadow-sm"
                onClick={logout}
              >
                Sign out
              </Button>
            </>
          ) : (
            <>
              <ThemeToggle compact surface="topbar" />
              <Button className="rounded-full px-4" size="sm" onClick={login}>
                Log in
              </Button>
            </>
          )}
        </div>
        {selectedVault ? (
          <VaultManagerPane
            vault={selectedVault}
            onBack={() => setSelectedVault(null)}
          />
        ) : isWalletPositionsActive ? (
          <WalletPositionsPane
            walletAddress={walletAddress}
            onBack={() => setIsWalletPositionsActive(false)}
            onActivityCountChange={handleWalletActivityCountChange}
          />
        ) : activeScreenerId ? (
          <PinnedScreenerPane
            screener={activeScreener}
            error={screenerError}
            isLoading={isLoadingScreener}
            onBack={() => {
              setActiveScreenerId(null);
              setActiveScreener(null);
              setScreenerError(null);
            }}
            onPinnedScreenersChange={refreshPinnedScreeners}
          />
        ) : (
          <ChatView
            key={strategyId}
            strategyId={strategyId}
            disabled={isCreatingStrategy}
            strategyError={strategyError}
            onBusyChange={setIsChatBusy}
            onKnownStrategiesChange={refreshKnownStrategies}
            onPinnedScreenersChange={refreshPinnedScreeners}
            onNewStrategy={handleNewStrategy}
            authenticated={authenticated}
            getAccessToken={getAccessToken}
          />
        )}
      </div>
    </main>
  );
}

function PinnedScreenerPane({
  screener,
  error,
  isLoading,
  onBack,
  onPinnedScreenersChange,
}: {
  screener: ScreenerResult | null;
  error: string | null;
  isLoading: boolean;
  onBack: () => void;
  onPinnedScreenersChange: () => void;
}) {
  return (
    <div className="min-h-0 flex-1 overflow-y-auto bg-background">
      <div className="sticky top-0 z-10 flex items-center justify-between gap-3 border-b border-border/60 bg-background/95 px-4 py-3 backdrop-blur">
        <div>
          <p className="font-mono text-[10px] uppercase tracking-[0.3em] text-muted-foreground">
            Pinned screener
          </p>
          <h1 className="font-heading text-lg font-semibold">
            Market watchlist
          </h1>
        </div>
        <Button variant="outline" size="sm" onClick={onBack}>
          Back to chat
        </Button>
      </div>
      <div className="mx-auto w-full max-w-[100rem] px-4 py-6 sm:px-8">
        {isLoading ? (
          <Card>
            <CardContent className="py-6 text-sm text-muted-foreground">
              Refreshing pinned screener...
            </CardContent>
          </Card>
        ) : error ? (
          <Card className="border-destructive/30 bg-destructive/10">
            <CardContent className="py-6 text-sm text-destructive">
              {error}
            </CardContent>
          </Card>
        ) : screener ? (
          <ScreenerResultCard
            result={screener}
            onPinnedScreenersChange={onPinnedScreenersChange}
          />
        ) : null}
      </div>
    </div>
  );
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
