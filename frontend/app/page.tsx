"use client";

import { startTransition, useEffect, useState } from "react";

import { ChatView } from "@/components/ChatView";
import { StrategySidebar } from "@/components/StrategySidebar";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  clearStrategyId,
  ensureKnownStrategy,
  getKnownStrategies,
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

async function requestStrategy() {
  const response = await fetch("/api/strategies", {
    method: "POST",
    cache: "no-store",
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

export default function Home() {
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
    let isActive = true;

    async function bootstrap() {
      setBootstrapError(null);
      setStrategyError(null);

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
        const next = await requestStrategy();

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
  }, [bootstrapKey]);

  async function handleNewStrategy() {
    if (isCreatingStrategy || isChatBusy) {
      return;
    }

    setStrategyError(null);
    setIsCreatingStrategy(true);

    try {
      const next = await requestStrategy();

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

  if (!strategyId && !bootstrapError) {
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

  return (
    <main className="min-h-screen bg-muted/30 px-4 py-4 sm:px-6 sm:py-6">
      <div className="mx-auto grid max-w-6xl gap-4 lg:grid-cols-[280px_minmax(0,1fr)]">
        <StrategySidebar
          strategies={knownStrategies}
          activeStrategyId={strategyId}
          disabled={isCreatingStrategy || isChatBusy}
          onSelectStrategy={handleSelectStrategy}
          onNewStrategy={handleNewStrategy}
        />

        <div className="min-w-0">
          <ChatView
            key={strategyId}
            strategyId={strategyId}
            disabled={isCreatingStrategy}
            strategyError={strategyError}
            onBusyChange={setIsChatBusy}
            onKnownStrategiesChange={refreshKnownStrategies}
            onNewStrategy={handleNewStrategy}
          />
        </div>
      </div>
    </main>
  );
}
