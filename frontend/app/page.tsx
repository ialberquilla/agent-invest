"use client";

import { useEffect, useState } from "react";

import { ChatView } from "@/components/ChatView";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  clearStrategyId,
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
  const [bootstrapError, setBootstrapError] = useState<string | null>(null);
  const [bootstrapKey, setBootstrapKey] = useState(0);

  useEffect(() => {
    let isActive = true;

    async function bootstrap() {
      setBootstrapError(null);

      const cachedStrategyId = getStrategyId();
      if (cachedStrategyId) {
        if (isActive) {
          setStrategyId(cachedStrategyId);
        }
        return;
      }

      try {
        const next = await requestStrategy();

        if (!isActive) {
          return;
        }

        persistStrategyId(next.strategy_id);
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
      <div className="mx-auto max-w-5xl">
        <ChatView initialStrategyId={strategyId} />
      </div>
    </main>
  );
}
