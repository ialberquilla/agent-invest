# Stage Evals

`run_stage_eval.py` is the shared Python harness for stage-level evals. It loads fixtures from `agent/evals/fixtures/<stage>/<name>.json`, dispatches to the scorer registered for the requested stage, prints per-rule pass/fail lines to stderr, and writes a JSON summary to stdout.

For task 022 the harness intentionally mocks the stage runner at the runner boundary instead of invoking the live TypeScript pipeline. This keeps the eval loop deterministic and usable before stage-specific scorers and golden fixtures exist. The tradeoff is that task 022 validates fixture loading, scorer dispatch, reporting, and optional persistence, but it does not measure live model or tool behavior until later tasks replace the placeholder scorer and runner boundary.

## Commands

Run one stage from the repo root:

```sh
pnpm eval:stage:thesis
pnpm eval:stage:designer
pnpm eval:stage:adjudicator
pnpm eval:stage:reporter
```

Run all stage evals serially:

```sh
pnpm eval:all
```

Each root script first runs `pnpm py:sync:frozen`, then executes `uv run --project agent/scripts python -m agent_invest_scripts.run_stage_eval --stage <stage>`.

The default pass threshold is 80%. The CLI exits non-zero when a stage's fixture pass rate is below the threshold. To override it for an ad hoc run, call the harness directly:

```sh
pnpm py:sync:frozen
uv run --project agent/scripts python -m agent_invest_scripts.run_stage_eval --stage thesis --threshold 1.0
```

Use `--save-to-db` to insert one `stage_eval_runs` row per fixture. `DATABASE_URL` must be set for persistence.

## Adding A Fixture

Add a JSON file under `agent/evals/fixtures/<stage>/<name>.json` with an `input` object and an `expectations` object. Keep the fixture focused on one behavior, then run the matching `pnpm eval:stage:<stage>` command to confirm the scorer accepts the new golden.

## Fixture Set Notes

Task 029 added three Designer round-2 fixtures with non-empty `refinement_reasons` and two Adjudicator ambiguous-batch fixtures with multiple acceptable winners.

Current mock-runner pass rates after task 029:

- Designer round-2 fixtures: 3/3 passing via `pnpm eval:stage:designer`.
- Designer full fixture set: 11/11 passing via `pnpm eval:stage:designer`.
- Adjudicator ambiguous-batch fixtures: 4/4 passing via `pnpm eval:stage:adjudicator`.
- Adjudicator full fixture set: 12/12 passing via `pnpm eval:stage:adjudicator`.
