"use client";

import type { ArtifactRef } from "@/lib/types";

const KIND_LABELS: Record<string, string> = {
  equity_curve_png: "Equity curve",
  drawdown_png: "Drawdown",
  report_json: "Report (JSON)",
};

function buildProxyUrl(artifactPath: string): string {
  const segments = artifactPath
    .split("/")
    .filter(Boolean)
    .map(encodeURIComponent)
    .join("/");
  return `/api/artifacts/${segments}`;
}

function isImagePath(artifactPath: string): boolean {
  return /\.(png|jpe?g|svg|gif|webp)$/i.test(artifactPath);
}

function labelFor(artifact: ArtifactRef): string {
  return KIND_LABELS[artifact.kind] ?? artifact.kind ?? artifact.path;
}

export type ArtifactGalleryProps = {
  artifacts: ArtifactRef[];
};

export function ArtifactGallery({ artifacts }: ArtifactGalleryProps) {
  if (!artifacts || artifacts.length === 0) return null;

  return (
    <div className="mt-3 space-y-3 border-t border-border/60 pt-3">
      {artifacts.map((artifact) => {
        const url = buildProxyUrl(artifact.path);
        const label = labelFor(artifact);

        if (isImagePath(artifact.path)) {
          return (
            <figure key={artifact.path} className="space-y-1">
              <a href={url} target="_blank" rel="noreferrer">
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img
                  src={url}
                  alt={label}
                  className="w-full rounded-md border border-border bg-background"
                  loading="lazy"
                />
              </a>
              <figcaption className="text-[11px] text-muted-foreground">
                {label}
              </figcaption>
            </figure>
          );
        }

        return (
          <a
            key={artifact.path}
            href={url}
            target="_blank"
            rel="noreferrer"
            className="block rounded-md border border-border bg-background px-3 py-2 text-xs hover:bg-muted"
          >
            <span className="font-medium">{label}</span>
            <span className="ml-2 text-muted-foreground">{artifact.path}</span>
          </a>
        );
      })}
    </div>
  );
}
