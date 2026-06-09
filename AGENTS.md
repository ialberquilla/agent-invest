# pond3r-portfolio

## What this project is

pond3r-portfolio is a **crypto portfolio research copilot**. It turns a vague investment
intent into an explicit, parameterized, GMX-executable strategy: it interprets a brief,
proposes candidate strategies, backtests them with realistic costs, validates them
against the user's thesis/risk limits, and finalizes a recommended strategy. The user
can then deploy an on-chain vault and execute the allocation manually (no autonomous
trading in the MVP).

It is positioned as a research / thesis-monitoring / decision-discipline tool — not an
alpha machine or financial advisor. The full product spec lives in `plans/spec.md`
(domain objects: Thesis, Strategy Template, Candidate Strategy, Walk-Forward Review,
Decision Check; the 13-strategy MVP set; GMX execution and validation model).

This is the user's third iteration on the problem, replacing two earlier over-engineered
repos. **Default to the smallest thing that works — no speculative abstractions.**

## Repository layout (pnpm + uv monorepo)

- `agent/` — `@pond3r-portfolio/agent`. The backend.
  - `src/api/server.ts` — Fastify HTTP API (runs, strategies, messages, events/SSE,
    artifacts, ingestion, eval inspection).
  - `src/agent/` — the agent layer driven by the **opencode SDK** (not the Anthropic
    SDK, not a CLI subprocess). `chat.ts` is the conversational agent; it exposes
    `run_strategy_pipeline`, `screen_markets`, and gated `run_research_code` tools.
  - `src/agent/workflow/` — the core. A deterministic **step-based controller**
    (`controller.ts`) that loops over discrete steps in `steps/`
    (`interpret_brief → select_templates → select_universe → select_window →
    propose_candidates → run_and_validate → decide → finalize`). It enforces per-edge
    counters and global caps, and always salvages a best-effort winner from any attempt
    that produced a backtest, only falling back to "no viable strategy" when nothing
    usable exists. Each step has a co-located `*.eval.ts`. (Note: an older single
    "strategist" agent design described in `plans/status.md` has been replaced by this.)
  - `src/agent/keeper/live-allocation.ts` — maps a persisted strategy mandate to a live
    allocation computation, reusing the backtest's own per-candidate path so the live
    target can never drift from the backtested strategy.
  - `src/db/` — Drizzle ORM over Postgres. Holds run metadata, session/thread IDs,
    events, structured tool calls, stage runs, theses, strategy mandates, and vault
    bindings — never memory/strategy content. Migrations in `agent/migrations/`.
  - `src/ingestion/` — out-of-band data ingestion (GMX history, CoinGecko market caps).
    The agent only reads ingested data; it does not fetch.
  - `scripts/` (Python, `pond3r-portfolio-scripts`) — the compute layer. The agent invokes
    these directly via opencode's shell tool through one dispatcher CLI
    (`python -m agent_invest_scripts.cli`, wrapper `agent/pond3r-portfolio`). Each
    subcommand: CLI flags in, JSON to stdout, errors as JSON. Crypto signal/backtest
    logic lives in `agent_invest_scripts/_lib/` (uses the `bt` backtesting library).
- `contracts/` — `@pond3r-portfolio/contracts`. Foundry/Solidity. `StrategyVault.sol` is a
  single-user (single-depositor, no shares/ERC-4626) upgradeable collateral vault that
  executes owner-approved GMX v2 perp orders (increase/decrease/cancel). The owner is the
  sole depositor and beneficiary; strategy intelligence stays off-chain and the vault only
  executes. Solc 0.8.26.
- `frontend/` — Next.js app (chat + beginner wizard, run inspector, eval dev tools).
  Uses Privy for auth and deploys the StrategyVault on-chain via a "deploy" CTA.
  **NOTE:** this is a non-standard Next.js — read `frontend/AGENTS.md`.

## Storage & data

Phase 1 storage is **local filesystem only** (no S3/AWS). Memory files and parquet
datasets live under `STORAGE_ROOT` (default `<repo>/.data/storage`, Docker
`/app/.data/storage`). See `README.md` for the layout. Postgres runs in Docker.

**Postgres is exposed on host port `5434`** (not 5432) to coexist with pond3r-postgres.

`run_research_code` is an opt-in research sandbox for open-ended quantitative
analysis. It requires `RESEARCH_MODE=true` and `AGENT_READONLY_DATABASE_URL`, and the
database role should be `agent_readonly` from the migrations: SELECT-only on market
data views/tables, statement timeout, and default read-only transactions. The tool
must remain a first-class MCP/API tool; do not give chat access to opencode `bash`.

## Common commands

```sh
pnpm dev                  # run the agent server (tsx watch)
pnpm test                 # agent TypeScript tests
pnpm --filter @pond3r-portfolio/agent typecheck
pnpm db:migrate           # apply Drizzle migrations
pnpm contracts:build / contracts:test / contracts:fmt
# Python tests: from agent/scripts, `uv run pytest`
# Agent compute CLI: from agent/, `./pond3r-portfolio list` / `help <cmd>` / `<cmd> [args]`
```

## CRITICAL: Package Manager Policy

Use `pnpm` for all JavaScript dependency operations. Do not use `npm` or `npx`.

Use the root scripts for Python dependency operations so uv applies the 7-day package age gate from `agent/scripts/pyproject.toml`:

```sh
pnpm py:lock
pnpm py:sync
pnpm py:sync:frozen
```

For production or CI installs, use `pnpm py:sync:frozen` so workloads stay aligned with `agent/scripts/uv.lock`.

## Secrets

Code auto-loads `.env` (TS `env.ts`, Python `db.py`). **Never grep, read, or export
`.env` secrets** — assume the runtime already has them.
