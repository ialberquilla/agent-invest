import { Check, Circle, X } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { StageRunSummary } from "@/lib/types";

type PipelineTimelineProps = {
  stages: StageRunSummary[];
};

type TimelineStatus = "pending" | "running" | "succeeded" | "failed";

const stageSlots = [
  { key: "thesis", label: "Thesis", rounds: false },
  { key: "designer", label: "Designer", rounds: true },
  { key: "adjudicator", label: "Adjudicator", rounds: true },
  { key: "reporter", label: "Reporter", rounds: false },
] as const;

const statusRank: Record<TimelineStatus, number> = {
  failed: 4,
  running: 3,
  succeeded: 2,
  pending: 1,
};

const statusClasses: Record<TimelineStatus, string> = {
  pending: "border-border bg-muted/40 text-muted-foreground",
  running: "border-blue-500/30 bg-blue-500/10 text-blue-700 dark:text-blue-300",
  succeeded: "border-emerald-500/30 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300",
  failed: "border-destructive/30 bg-destructive/10 text-destructive",
};

const roundLabels: Record<TimelineStatus, string> = {
  pending: "-",
  running: "active",
  succeeded: "done",
  failed: "failed",
};

function normalizeStageName(stage: string) {
  return stage.toLowerCase().replace(/[_\s-]+/g, "");
}

function normalizeStatus(status?: string): TimelineStatus {
  if (status === "running") return "running";
  if (status === "succeeded" || status === "success" || status === "completed") {
    return "succeeded";
  }
  if (status === "failed" || status === "error") return "failed";
  return "pending";
}

function summarizeStatus(stages: StageRunSummary[]) {
  return stages.reduce<TimelineStatus>((summary, stage) => {
    const status = normalizeStatus(stage.status);
    return statusRank[status] > statusRank[summary] ? status : summary;
  }, "pending");
}

function latestStageRun(stages: StageRunSummary[], round: number) {
  return stages
    .filter((stage) => stage.round === round)
    .sort((left, right) => right.started_at.localeCompare(left.started_at))[0];
}

function statusIcon(status: TimelineStatus) {
  if (status === "succeeded") return <Check aria-hidden="true" className="size-3" />;
  if (status === "failed") return <X aria-hidden="true" className="size-3" />;
  if (status === "running") return <Circle aria-hidden="true" className="size-3 fill-current" />;
  return null;
}

function stagePanelAnchor(stage: string, round: number) {
  return `#stage-${stage}-${round}`;
}

export function PipelineTimeline({ stages }: PipelineTimelineProps) {
  return (
    <div className="grid gap-3 md:grid-cols-4" aria-label="Pipeline timeline">
      {stageSlots.map((slot, index) => {
        const slotStages = stages.filter(
          (stage) => normalizeStageName(stage.stage) === slot.key,
        );
        const slotStatus = summarizeStatus(slotStages);

        return (
          <section
            key={slot.key}
            className="relative rounded-2xl border bg-muted/20 p-4"
            aria-label={`${slot.label} stage`}
          >
            {index < stageSlots.length - 1 ? (
              <div className="absolute top-8 left-[calc(100%-0.75rem)] hidden h-px w-6 bg-border md:block" />
            ) : null}
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-sm font-semibold">{slot.label}</p>
                <p className="mt-1 text-xs text-muted-foreground">
                  {slotStages.length ? `${slotStages.length} run${slotStages.length === 1 ? "" : "s"}` : "Not started"}
                </p>
              </div>
              <Badge
                variant="outline"
                className={cn("capitalize", statusClasses[slotStatus])}
              >
                {statusIcon(slotStatus)}
                {slotStatus}
              </Badge>
            </div>

            {slot.rounds ? (
              <div className="mt-4 flex flex-wrap gap-2" aria-label={`${slot.label} rounds`}>
                {[1, 2, 3].map((round) => {
                  const roundStage = latestStageRun(slotStages, round);
                  const roundStatus = normalizeStatus(roundStage?.status);

                  return (
                    <a
                      key={round}
                      href={stagePanelAnchor(slot.key, round)}
                      className={cn(
                        "inline-flex h-7 items-center gap-1 rounded-full border px-2 text-xs font-medium outline-none transition-colors focus-visible:ring-3 focus-visible:ring-ring/50",
                        statusClasses[roundStatus],
                      )}
                      aria-label={`${slot.label} round ${round} ${roundStatus}`}
                    >
                      <span>r{round}</span>
                      <span>{roundLabels[roundStatus]}</span>
                    </a>
                  );
                })}
              </div>
            ) : null}
          </section>
        );
      })}
    </div>
  );
}
