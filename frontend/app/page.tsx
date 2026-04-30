"use client";

import { useEffect, useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";

type HealthResponse = {
  ok: boolean;
};

const POLL_INTERVAL_MS = 10_000;

export default function Home() {
  const [isHealthy, setIsHealthy] = useState<boolean | null>(null);

  useEffect(() => {
    let isActive = true;

    async function pollHealth() {
      try {
        const response = await fetch("/api/health", { cache: "no-store" });
        const data = (await response.json()) as HealthResponse;

        if (isActive) {
          setIsHealthy(Boolean(data.ok));
        }
      } catch {
        if (isActive) {
          setIsHealthy(false);
        }
      }
    }

    pollHealth();

    const intervalId = window.setInterval(pollHealth, POLL_INTERVAL_MS);

    return () => {
      isActive = false;
      window.clearInterval(intervalId);
    };
  }, []);

  const badgeClassName =
    isHealthy === null
      ? "border-zinc-300 bg-zinc-100 text-zinc-700 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-300"
      : isHealthy
        ? "border-emerald-200 bg-emerald-100 text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-300"
        : "border-rose-200 bg-rose-100 text-rose-800 dark:border-rose-900 dark:bg-rose-950 dark:text-rose-300";

  const badgeLabel =
    isHealthy === null
      ? "agent: checking"
      : `agent: ${isHealthy ? "ok" : "down"}`;

  return (
    <main className="flex min-h-screen items-center justify-center bg-zinc-50 px-6 py-24 font-sans dark:bg-black">
      <Card className="w-full max-w-md border-zinc-200 bg-white p-8 shadow-sm dark:border-zinc-800 dark:bg-zinc-950">
        <div className="flex flex-col gap-4">
          <p className="text-sm font-medium uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">
            Agent Proxy Check
          </p>
          <div className="flex items-center justify-between gap-4">
            <div>
              <h1 className="text-2xl font-semibold tracking-tight text-zinc-950 dark:text-zinc-50">
                Health status
              </h1>
              <p className="mt-1 text-sm text-zinc-600 dark:text-zinc-400">
                Polls the local proxy every 10 seconds.
              </p>
            </div>
            <Badge className={badgeClassName}>{badgeLabel}</Badge>
          </div>
        </div>
      </Card>
    </main>
  );
}
