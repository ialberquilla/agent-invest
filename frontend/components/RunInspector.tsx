"use client";

import { useEffect, useState } from "react";

import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Run } from "@/lib/types";

type RunInspectorProps = {
  open: boolean;
  runId: string | null;
  onOpenChange: (open: boolean) => void;
};

type LoadState =
  | { status: "idle" | "loading" }
  | { status: "success"; run: Run }
  | { status: "not-found" }
  | { status: "error"; message: string };

const RUN_FIELDS: Array<keyof Run> = [
  "run_id",
  "status",
  "started_at",
  "ended_at",
  "exit_code",
  "reply",
  "error",
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isRun(value: unknown): value is Run {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.run_id === "string" &&
    typeof value.status === "string" &&
    typeof value.started_at === "string" &&
    (typeof value.ended_at === "string" || value.ended_at === null) &&
    (typeof value.exit_code === "number" || value.exit_code === null) &&
    (typeof value.reply === "string" || value.reply === null) &&
    (typeof value.error === "string" || value.error === null)
  );
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

  return "Unable to load run";
}

function formatValue(value: Run[keyof Run]) {
  return JSON.stringify(value, null, 2) ?? "null";
}

export function RunInspector({ open, runId, onOpenChange }: RunInspectorProps) {
  const [state, setState] = useState<LoadState>({ status: "idle" });

  useEffect(() => {
    if (!open || !runId) {
      return;
    }

    const abortController = new AbortController();
    setState({ status: "loading" });

    async function loadRun() {
      try {
        const response = await fetch(`/api/runs/${encodeURIComponent(runId)}`, {
          cache: "no-store",
          signal: abortController.signal,
        });
        const payload = await readJson(response);

        if (response.status === 404) {
          setState({ status: "not-found" });
          return;
        }

        if (!response.ok) {
          setState({ status: "error", message: getErrorMessage(payload) });
          return;
        }

        if (!isRun(payload)) {
          setState({ status: "error", message: "Run response was invalid" });
          return;
        }

        setState({ status: "success", run: payload });
      } catch (error) {
        if ((error as Error).name === "AbortError") {
          return;
        }

        setState({ status: "error", message: "Unable to load run" });
      }
    }

    void loadRun();

    return () => {
      abortController.abort();
    };
  }, [open, runId]);

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full p-0 sm:max-w-xl">
        <SheetHeader className="border-b pr-14">
          <SheetTitle>Run inspector</SheetTitle>
          <SheetDescription>
            {runId
              ? `Inspecting ${runId}`
              : "Select an agent response to inspect its run."}
          </SheetDescription>
        </SheetHeader>

        <ScrollArea className="min-h-0 flex-1">
          <div className="space-y-4 p-4">
            {state.status === "idle" || state.status === "loading" ? (
              <p className="text-sm text-muted-foreground">
                Loading run details...
              </p>
            ) : null}

            {state.status === "not-found" ? (
              <p className="text-sm text-muted-foreground">run not found</p>
            ) : null}

            {state.status === "error" ? (
              <p className="text-sm text-destructive">{state.message}</p>
            ) : null}

            {state.status === "success" ? (
              <div className="space-y-3">
                {RUN_FIELDS.map((field) => (
                  <div key={field} className="space-y-1">
                    <p className="text-xs font-medium tracking-wide text-muted-foreground uppercase">
                      {field}
                    </p>
                    <pre className="overflow-x-auto rounded-md border bg-muted/40 p-3 font-mono text-xs leading-6 whitespace-pre-wrap break-words">
                      {formatValue(state.run[field])}
                    </pre>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </ScrollArea>
      </SheetContent>
    </Sheet>
  );
}
