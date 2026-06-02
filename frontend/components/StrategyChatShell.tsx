"use client";

import { startTransition, useEffect, useState } from "react";
import { usePrivy } from "@privy-io/react-auth";

import { ChatView } from "@/components/ChatView";
import { StrategySidebar } from "@/components/StrategySidebar";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  clearStrategyId,
  ensureKnownStrategy,
  getKnownStrategies,
  getAnonymousUserId,
  getStrategyId,
  setStrategyId as persistStrategyId,
} from "@/lib/local-store";
import { StrategyCreateResponse } from "@/lib/types";

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

async function authHeaders(getAccessToken: () => Promise<string | null>) {
  const token = await getAccessToken();
  return token ? { authorization: `Bearer ${token}` } : undefined;
}

async function requestStrategy(getAccessToken: () => Promise<string | null>) {
  const anonymousUserId = getAnonymousUserId();
  const response = await fetch("/api/strategies", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(await authHeaders(getAccessToken)),
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
  const { ready, authenticated, user, login, logout, getAccessToken } =
    usePrivy();
  const [strategyId, setStrategyId] = useState<string | null>(null);
  const [knownStrategies, setKnownStrategies] = useState(() =>
    getKnownStrategies(),
  );
  const [bootstrapError, setBootstrapError] = useState<string | null>(null);
  const [strategyError, setStrategyError] = useState<string | null>(null);
  const [bootstrapKey, setBootstrapKey] = useState(0);
  const [isCreatingStrategy, setIsCreatingStrategy] = useState(false);
  const [isChatBusy, setIsChatBusy] = useState(false);

  function refreshKnownStrategies() {
    setKnownStrategies(getKnownStrategies());
  }

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
          ...(await authHeaders(getAccessToken)),
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
        const next = await requestStrategy(getAccessToken);

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
  }, [bootstrapKey, ready, getAccessToken]);

  async function handleNewStrategy() {
    if (isCreatingStrategy || isChatBusy) {
      return;
    }

    setStrategyError(null);
    setIsCreatingStrategy(true);

    try {
      const next = await requestStrategy(getAccessToken);

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
    if (isCreatingStrategy || isChatBusy || nextStrategyId === strategyId) {
      return;
    }

    setStrategyError(null);
    persistStrategyId(nextStrategyId);
    startTransition(() => {
      setStrategyId(nextStrategyId);
    });
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
        activeStrategyId={strategyId}
        disabled={isCreatingStrategy || isChatBusy}
        onSelectStrategy={handleSelectStrategy}
        onNewStrategy={handleNewStrategy}
      />

      <div className="min-w-0 flex-1">
        <div className="flex items-center justify-end gap-3 border-b border-border/60 px-4 py-2 text-sm">
          {authenticated ? (
            <>
              <span className="truncate text-muted-foreground">
                {user?.email?.address ?? user?.wallet?.address ?? user?.id}
              </span>
              <Button variant="outline" size="sm" onClick={logout}>
                Log out
              </Button>
            </>
          ) : (
            <Button variant="outline" size="sm" onClick={login}>
              Log in
            </Button>
          )}
        </div>
        <ChatView
          key={strategyId}
          strategyId={strategyId}
          disabled={isCreatingStrategy}
          strategyError={strategyError}
          onBusyChange={setIsChatBusy}
          onKnownStrategiesChange={refreshKnownStrategies}
          onNewStrategy={handleNewStrategy}
          getAccessToken={getAccessToken}
        />
      </div>
    </main>
  );
}
