"use client";

import { useEffect, useState } from "react";

import { LiveActivity } from "@/components/LiveActivity";
import { Badge } from "@/components/ui/badge";
import { Run } from "@/lib/types";

type StreamStatus = "connecting" | "open" | "closed" | "error";

type RunDetailClientProps = {
  run: Run;
};

export function RunDetailClient({ run }: RunDetailClientProps) {
  const [streamStatus, setStreamStatus] = useState<StreamStatus>("connecting");

  useEffect(() => {
    const source = new EventSource(
      `/api/runs/${encodeURIComponent(run.run_id)}/stream`,
    );

    source.addEventListener("open", () => {
      setStreamStatus("open");
    });

    source.addEventListener("error", () => {
      setStreamStatus(
        source.readyState === EventSource.CLOSED ? "closed" : "error",
      );
    });

    return () => {
      setStreamStatus("closed");
      source.close();
    };
  }, [run.run_id]);

  return (
    <main className="min-h-screen bg-background px-4 py-8 text-foreground sm:px-6 lg:px-8">
      <div className="mx-auto flex w-full max-w-4xl flex-col gap-6">
        <header className="flex flex-wrap items-center justify-between gap-3">
          <div className="min-w-0">
            <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Run
            </p>
            <h1 className="truncate font-mono text-lg font-semibold">
              {run.run_id}
            </h1>
          </div>
          <div className="flex flex-wrap gap-2">
            <Badge variant="outline">run {run.status}</Badge>
            <Badge variant={streamStatus === "open" ? "secondary" : "outline"}>
              stream {streamStatus}
            </Badge>
          </div>
        </header>

        <LiveActivity runId={run.run_id} fullWidth includeText />
      </div>
    </main>
  );
}
