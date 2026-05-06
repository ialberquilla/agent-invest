import Link from "next/link";
import type { ReactNode } from "react";
import { Bot, Command, Search } from "lucide-react";

type DashboardShellProps = {
  eyebrow: string;
  title: string;
  description: string;
  actions?: ReactNode;
  children: ReactNode;
};

export function DashboardShell({
  eyebrow,
  title,
  description,
  actions,
  children,
}: DashboardShellProps) {
  return (
    <main className="min-h-dvh bg-[radial-gradient(circle_at_top_left,color-mix(in_oklab,var(--primary)_12%,transparent),transparent_30%),var(--background)] text-foreground">
      <header className="sticky top-0 z-20 border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/75">
        <div className="mx-auto flex min-h-14 max-w-7xl items-center gap-4 px-4 sm:px-6 lg:px-8">
          <Link href="/" className="flex shrink-0 items-center gap-3">
            <span className="flex size-8 items-center justify-center rounded-lg bg-primary text-primary-foreground shadow-sm">
              <Bot className="size-4" />
            </span>
            <span className="font-heading text-sm font-bold tracking-tight">
              <span>Agent</span>
              <span className="text-muted-foreground">Invest</span>
            </span>
          </Link>

          <div className="ml-auto flex items-center gap-3">
            <div className="hidden items-center gap-2 rounded-md border bg-muted/40 px-3 py-2 text-xs text-muted-foreground shadow-xs sm:flex lg:min-w-64">
              <Search className="size-3.5 shrink-0" />
              <span className="truncate">
                Search wizard inputs or reports...
              </span>
              <kbd className="ml-auto inline-flex items-center gap-1 rounded border bg-background px-1.5 py-0.5 font-mono text-[10px]">
                <Command className="size-2.5" />K
              </kbd>
            </div>

            <div className="flex items-center gap-1.5">
              <span className="relative flex size-2">
                <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-500 opacity-40" />
                <span className="relative inline-flex size-2 rounded-full bg-emerald-500" />
              </span>
              <span className="hidden font-mono text-[10px] uppercase tracking-widest text-emerald-600 dark:text-emerald-400 sm:inline">
                Live
              </span>
            </div>
          </div>
        </div>

        <div className="border-t">
          <div className="mx-auto flex max-w-7xl flex-col gap-4 px-4 py-6 sm:px-6 lg:px-8 xl:flex-row xl:items-end xl:justify-between">
            <div className="max-w-3xl space-y-2">
              <p className="font-mono text-[10px] uppercase tracking-[0.3em] text-muted-foreground">
                {eyebrow}
              </p>
              <h1 className="font-heading text-2xl font-bold tracking-tight sm:text-3xl">
                {title}
              </h1>
              <p className="text-sm leading-6 text-muted-foreground">
                {description}
              </p>
            </div>
            {actions ? (
              <div className="flex flex-wrap gap-2">{actions}</div>
            ) : null}
          </div>
        </div>
      </header>

      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
        {children}
      </div>
    </main>
  );
}
