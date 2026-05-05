# DB Loading Plan

## Goal

Move the agent-facing Python datasets from parquet-backed files to database-backed readers.

The primary datasets needed by the agent are:

- A current asset universe ranked by latest market cap/rank.
- A daily close-price dataset for backtesting.
- A daily-refreshed feature table/materialized view to help the agent shortlist coins before backtesting.

Volume, OHLC fields, historical daily market caps, and separate coin metadata are not required for the agent path.

## Current State

The agent exposes Python scripts through `src/agent/prompt.ts`:

- `list_universe`
- `rank_universe` should be added
- `run_backtest`
- `finalize_strategy_result`
- `list_runs`

The data used by those scripts is loaded through `scripts/agent_invest_scripts/_lib/data.py`.

That module currently reads parquet datasets from `STORAGE_ROOT/datasets`:

- `universe_history.parquet`
- `daily_prices.parquet`
- `daily_market_caps.parquet`
- `daily_volumes.parquet`
- `coin_metadata.parquet`

The desired change is to preserve the Python dataset functions while changing their backing source to Postgres.

## Relevant DB Tables

The existing schema already contains the required source tables:

- `assets`
- `asset_prices`
- `asset_source_mappings`

Relevant fields:

```text
assets:
  asset_id
  source
  source_asset_id
  symbol
  name
  market_cap_rank
  metadata

asset_prices:
  asset_id
  timestamp
  source
  close
  metadata

asset_source_mappings:
  asset_id
  source
  source_asset_id
```

`asset_market_caps` exists, but it should not be treated as a historical market-cap dataset for the agent. The DB currently only has the latest market-cap/rank state available for universe selection, through `assets.market_cap_rank` and any latest market-cap value stored in `assets.metadata` if present.

## Required Dataset Shapes

Keep these Python functions available:

```python
universe_history() -> pd.DataFrame
daily_prices() -> pd.DataFrame
```

`daily_market_caps()`, `daily_volumes()`, and `coin_metadata()` are not needed for the target agent path. They can be removed from active use or kept as compatibility wrappers only if tests or old scripts still import them.

### `daily_prices`

Required by `run_backtest`.

Columns:

```text
date
coin_id
price
```

Rules:

- `date` is `asset_prices.timestamp::date`.
- `coin_id` should be the CoinGecko ID when available.
- `price` is `asset_prices.close`.
- Only rows with non-null `close` are needed.
- If multiple close prices exist for the same `(date, coin_id)`, keep one deterministic row, preferably the latest timestamp on that date.

### `asset_universe`

Required by `list_universe`.

Target columns:

```text
coin_id
symbol
name
market_cap_rank
market_cap
price
```

Rules:

- Build from `assets` joined to `asset_source_mappings` for CoinGecko IDs.
- Use `assets.market_cap_rank` as the primary ranking field.
- Use latest market cap from `assets.metadata` only if available.
- Join the latest close price per asset from `asset_prices` for context.
- Do not require daily historical market-cap rows.
- Do not require volume.
- `coin_id` must match the ID accepted by `run_backtest` allocations.

### `universe_history`

The current `list_universe` implementation expects a dataset-like frame with a date column. Since the DB only has the latest market-cap/rank state, there are two options:

1. Preferable: change `list_universe` to call a new `asset_universe()` reader instead of `universe_history()`.
2. Compatibility: keep `universe_history()` but have it return the latest asset universe with a synthetic `date` value, such as `current_date` or the latest available price date.

For clarity, the implementation should prefer option 1 and rename the Python reader around the real data semantics.

Compatibility columns if `universe_history()` is kept temporarily:

```text
date
coin_id
symbol
name
market_cap_rank
market_cap
price
```

## Proposed SQL Views

Create DB views to centralize the join logic and keep Python simple.

### `agent_asset_universe`

Purpose: expose the current selectable universe for the agent.

```sql
CREATE VIEW agent_asset_universe AS
WITH latest_prices AS (
  SELECT DISTINCT ON (ap.asset_id)
    ap.asset_id,
    ap.close::double precision AS latest_price,
    ap.timestamp AS latest_price_at
  FROM asset_prices ap
  WHERE ap.close IS NOT NULL
  ORDER BY ap.asset_id, ap.timestamp DESC
)
SELECT
  a.asset_id,
  COALESCE(cg.source_asset_id, a.source_asset_id) AS coin_id,
  lower(a.symbol) AS symbol,
  a.name,
  a.market_cap_rank,
  NULLIF(a.metadata->>'market_cap', '')::double precision AS market_cap,
  lp.latest_price AS price,
  lp.latest_price_at
FROM assets a
LEFT JOIN asset_source_mappings cg
  ON cg.asset_id = a.asset_id
 AND cg.source = 'coingecko'
LEFT JOIN latest_prices lp
  ON lp.asset_id = a.asset_id
WHERE a.market_cap_rank IS NOT NULL;
```

If `assets.metadata->>'market_cap'` is not populated, keep `market_cap` nullable and rank by `market_cap_rank`.

### `agent_daily_close_prices`

Purpose: expose one daily close price per coin.

```sql
CREATE VIEW agent_daily_close_prices AS
SELECT DISTINCT ON (DATE(ap.timestamp), m.coin_id)
  DATE(ap.timestamp) AS date,
  m.coin_id,
  ap.close::double precision AS price,
  ap.timestamp,
  ap.source
FROM asset_prices ap
JOIN agent_asset_universe m
  ON m.asset_id = ap.asset_id
WHERE ap.close IS NOT NULL
ORDER BY DATE(ap.timestamp), m.coin_id, ap.timestamp DESC;
```

### Optional Compatibility View: `agent_universe_history`

Purpose: keep old `list_universe` code working while the Python API is migrated to `asset_universe()`.

```sql
CREATE VIEW agent_universe_history AS
SELECT
  CURRENT_DATE AS date,
  coin_id,
  symbol,
  name,
  market_cap,
  market_cap_rank,
  price
FROM agent_asset_universe;
```

### `agent_asset_universe_features`

Purpose: expose one current row per coin with precomputed selection features. The agent can use this to shortlist assets before running a backtest.

This should be a materialized view, refreshed once per day after price ingestion completes.

Target columns:

```text
asset_id
coin_id
symbol
name
market_cap_rank
market_cap
latest_price
first_price_date
last_price_date
data_days_365d
return_30d
return_90d
return_180d
return_365d
volatility_30d
volatility_90d
volatility_180d
max_drawdown_180d
sharpe_180d
price_above_sma_200d
```

Feature definitions:

- `latest_price`: latest available daily close price.
- `first_price_date`: first available close-price date for the coin.
- `last_price_date`: latest available close-price date for the coin.
- `data_days_365d`: count of daily close-price observations in the trailing 365 days.
- `return_30d`: latest close divided by close at least 30 days before latest date, minus 1.
- `return_90d`: latest close divided by close at least 90 days before latest date, minus 1.
- `return_180d`: latest close divided by close at least 180 days before latest date, minus 1.
- `return_365d`: latest close divided by close at least 365 days before latest date, minus 1.
- `volatility_30d`: annualized standard deviation of daily returns over trailing 30 days.
- `volatility_90d`: annualized standard deviation of daily returns over trailing 90 days.
- `volatility_180d`: annualized standard deviation of daily returns over trailing 180 days.
- `max_drawdown_180d`: worst peak-to-trough drawdown over trailing 180 days.
- `sharpe_180d`: annualized mean daily return divided by annualized volatility over trailing 180 days, using zero risk-free rate.
- `price_above_sma_200d`: boolean indicating latest close is above the trailing 200-day simple moving average.

Do not include `distance_from_ath_365d` in the initial implementation.

Suggested materialized view sketch:

```sql
CREATE MATERIALIZED VIEW agent_asset_universe_features AS
WITH prices AS (
  SELECT
    p.date,
    p.coin_id,
    p.price,
    lag(p.price) OVER (PARTITION BY p.coin_id ORDER BY p.date) AS previous_price
  FROM agent_daily_close_prices p
),
returns AS (
  SELECT
    date,
    coin_id,
    price,
    CASE
      WHEN previous_price IS NULL OR previous_price = 0 THEN NULL
      ELSE (price / previous_price) - 1
    END AS daily_return
  FROM prices
),
latest AS (
  SELECT DISTINCT ON (coin_id)
    coin_id,
    date AS last_price_date,
    price AS latest_price
  FROM prices
  ORDER BY coin_id, date DESC
),
history AS (
  SELECT
    p.coin_id,
    min(p.date) AS first_price_date,
    count(*) FILTER (WHERE p.date >= l.last_price_date - INTERVAL '365 days') AS data_days_365d,
    avg(p.price) FILTER (WHERE p.date >= l.last_price_date - INTERVAL '200 days') AS sma_200d,
    stddev_samp(r.daily_return) FILTER (WHERE r.date > l.last_price_date - INTERVAL '30 days') * sqrt(365) AS volatility_30d,
    stddev_samp(r.daily_return) FILTER (WHERE r.date > l.last_price_date - INTERVAL '90 days') * sqrt(365) AS volatility_90d,
    stddev_samp(r.daily_return) FILTER (WHERE r.date > l.last_price_date - INTERVAL '180 days') * sqrt(365) AS volatility_180d,
    avg(r.daily_return) FILTER (WHERE r.date > l.last_price_date - INTERVAL '180 days') * 365 AS annualized_return_180d
  FROM prices p
  JOIN latest l ON l.coin_id = p.coin_id
  LEFT JOIN returns r
    ON r.coin_id = p.coin_id
   AND r.date = p.date
  GROUP BY p.coin_id
),
lookback_prices AS (
  SELECT
    l.coin_id,
    p30.price AS price_30d,
    p90.price AS price_90d,
    p180.price AS price_180d,
    p365.price AS price_365d
  FROM latest l
  LEFT JOIN LATERAL (
    SELECT price FROM prices p
    WHERE p.coin_id = l.coin_id AND p.date <= l.last_price_date - INTERVAL '30 days'
    ORDER BY p.date DESC
    LIMIT 1
  ) p30 ON true
  LEFT JOIN LATERAL (
    SELECT price FROM prices p
    WHERE p.coin_id = l.coin_id AND p.date <= l.last_price_date - INTERVAL '90 days'
    ORDER BY p.date DESC
    LIMIT 1
  ) p90 ON true
  LEFT JOIN LATERAL (
    SELECT price FROM prices p
    WHERE p.coin_id = l.coin_id AND p.date <= l.last_price_date - INTERVAL '180 days'
    ORDER BY p.date DESC
    LIMIT 1
  ) p180 ON true
  LEFT JOIN LATERAL (
    SELECT price FROM prices p
    WHERE p.coin_id = l.coin_id AND p.date <= l.last_price_date - INTERVAL '365 days'
    ORDER BY p.date DESC
    LIMIT 1
  ) p365 ON true
),
drawdowns AS (
  SELECT
    coin_id,
    min(drawdown) AS max_drawdown_180d
  FROM (
    SELECT
      p.coin_id,
      p.date,
      (p.price / max(p.price) OVER (
        PARTITION BY p.coin_id
        ORDER BY p.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )) - 1 AS drawdown
    FROM prices p
    JOIN latest l ON l.coin_id = p.coin_id
    WHERE p.date > l.last_price_date - INTERVAL '180 days'
  ) rolling
  GROUP BY coin_id
)
SELECT
  u.asset_id,
  u.coin_id,
  u.symbol,
  u.name,
  u.market_cap_rank,
  u.market_cap,
  l.latest_price,
  h.first_price_date,
  l.last_price_date,
  h.data_days_365d,
  CASE WHEN lp.price_30d IS NULL OR lp.price_30d = 0 THEN NULL ELSE (l.latest_price / lp.price_30d) - 1 END AS return_30d,
  CASE WHEN lp.price_90d IS NULL OR lp.price_90d = 0 THEN NULL ELSE (l.latest_price / lp.price_90d) - 1 END AS return_90d,
  CASE WHEN lp.price_180d IS NULL OR lp.price_180d = 0 THEN NULL ELSE (l.latest_price / lp.price_180d) - 1 END AS return_180d,
  CASE WHEN lp.price_365d IS NULL OR lp.price_365d = 0 THEN NULL ELSE (l.latest_price / lp.price_365d) - 1 END AS return_365d,
  h.volatility_30d,
  h.volatility_90d,
  h.volatility_180d,
  d.max_drawdown_180d,
  CASE WHEN h.volatility_180d IS NULL OR h.volatility_180d = 0 THEN NULL ELSE h.annualized_return_180d / h.volatility_180d END AS sharpe_180d,
  CASE WHEN h.sma_200d IS NULL THEN NULL ELSE l.latest_price > h.sma_200d END AS price_above_sma_200d
FROM agent_asset_universe u
LEFT JOIN latest l ON l.coin_id = u.coin_id
LEFT JOIN history h ON h.coin_id = u.coin_id
LEFT JOIN lookback_prices lp ON lp.coin_id = u.coin_id
LEFT JOIN drawdowns d ON d.coin_id = u.coin_id;
```

Required indexes:

```sql
CREATE UNIQUE INDEX agent_asset_universe_features_coin_id_idx
  ON agent_asset_universe_features (coin_id);

CREATE INDEX agent_asset_universe_features_rank_idx
  ON agent_asset_universe_features (market_cap_rank);
```

Daily refresh:

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY agent_asset_universe_features;
```

If concurrent refresh is not needed or the unique index is not available yet, use:

```sql
REFRESH MATERIALIZED VIEW agent_asset_universe_features;
```

## Python Implementation Plan

1. Add DB connection helpers in `scripts/agent_invest_scripts/_lib/db.py`.
2. Load `.env` from the repo root or `agent/.env`, matching the TypeScript DB client behavior.
3. Use `DATABASE_URL` if set; otherwise use `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, and `PGPASSWORD`.
4. Implement `read_sql_frame(sql: str, params: dict | None = None) -> pd.DataFrame` using `psycopg` and `pandas.read_sql_query` or a cursor-to-DataFrame helper.
5. Replace parquet reads in `_lib/data.py` with DB-backed queries against the views.
6. Add `asset_universe() -> pd.DataFrame` and update `list_universe` to use it directly.
7. Keep `universe_history()` temporarily only if it reduces migration risk.

Suggested `_lib/data.py` mapping:

```python
def daily_prices() -> pd.DataFrame:
    return read_sql_frame(
        """
        SELECT date, coin_id, price
        FROM agent_daily_close_prices
        ORDER BY date, coin_id
        """
    )


def asset_universe() -> pd.DataFrame:
    return read_sql_frame(
        """
        SELECT coin_id, symbol, name, market_cap, market_cap_rank, price
        FROM agent_asset_universe
        ORDER BY market_cap_rank NULLS LAST, coin_id
        """
    )


def asset_universe_features() -> pd.DataFrame:
    return read_sql_frame(
        """
        SELECT
          asset_id,
          coin_id,
          symbol,
          name,
          market_cap_rank,
          market_cap,
          latest_price,
          first_price_date,
          last_price_date,
          data_days_365d,
          return_30d,
          return_90d,
          return_180d,
          return_365d,
          volatility_30d,
          volatility_90d,
          volatility_180d,
          max_drawdown_180d,
          sharpe_180d,
          price_above_sma_200d
        FROM agent_asset_universe_features
        ORDER BY market_cap_rank NULLS LAST, coin_id
        """
    )
```

## Agent Tool Changes

Add a new agent-facing Python script for the feature universe. Parity with the old parquet flow only needs `list_universe` and `run_backtest`, but the feature materialized view should be exposed as a tool so the agent can make better candidate selections before backtesting.

The existing tools should keep working once the dataset functions become DB-backed, with one small code change:

- `list_universe` should use `asset_universe()` instead of `universe_history()` and should rank by `market_cap_rank`, falling back to `market_cap` when rank is missing.
- Add `rank_universe`, backed by `asset_universe_features()`, to shortlist coins by trend, risk, market-cap rank, and data quality.
- `run_backtest` uses `daily_prices()`.

### `rank_universe` Script

Purpose: return ranked candidate coins from the daily-refreshed feature universe.

Recommended signature:

```text
--top-n <count>
[--max-market-cap-rank <rank>]
[--min-data-days-365d <days>]
[--positive-trend-only]
[--max-volatility-180d <value>]
[--sort market_cap_rank|momentum_180d|sharpe_180d|low_volatility]
```

Recommended default behavior:

- `--top-n` is required.
- Default sort should be `market_cap_rank`.
- Default `--min-data-days-365d` should be conservative, such as `180`, so the agent avoids assets with weak history.
- `--positive-trend-only` should require `return_180d > 0` and `price_above_sma_200d = true` when those values are available.
- Keep rows with missing feature values only when the selected sort does not require those values.

Output fields:

```text
rank
coin_id
symbol
name
market_cap_rank
market_cap
latest_price
return_30d
return_90d
return_180d
return_365d
volatility_30d
volatility_90d
volatility_180d
max_drawdown_180d
sharpe_180d
price_above_sma_200d
data_days_365d
first_price_date
last_price_date
```

The script should print structured JSON to stdout and errors to stderr, matching the existing agent script contract.

### Prompt Manifest Updates

Update `src/agent/prompt.ts` so `list_universe` no longer says “dataset cache” if the source is now Postgres.

Recommended wording:

```text
List the top-N coins by market cap from the database-backed universe dataset.
```

Add `rank_universe` to `AGENT_SCRIPT_REGISTRY`:

```text
name: rank_universe
summary: Rank candidate coins from the daily-refreshed DB feature universe for portfolio construction.
signature: --top-n <count> [--max-market-cap-rank <rank>] [--min-data-days-365d <days>] [--positive-trend-only] [--max-volatility-180d <value>] [--sort market_cap_rank|momentum_180d|sharpe_180d|low_volatility]
example: uv run --project agent/scripts python -m agent_invest_scripts.rank_universe --top-n 20 --max-market-cap-rank 100 --min-data-days-365d 180 --positive-trend-only --sort sharpe_180d
note: Use this before run_backtest when selecting candidate coins. It is a screening tool, not a substitute for backtesting.
```

Agent guidance to add near the tool manifest:

```text
Use `rank_universe` to shortlist candidate coins when the user asks for a portfolio, allocation, recommendation, comparison, or refinement and has not specified exact coins. Then run `run_backtest` on the selected allocation before finalizing the answer.
```

## Migration Plan

1. Add a Drizzle migration that creates the two required agent-facing views and one materialized feature view:
   - `agent_daily_close_prices`
   - `agent_asset_universe`
   - `agent_asset_universe_features`
2. Add Python DB helper module.
3. Replace parquet-backed readers in `_lib/data.py`.
4. Update `list_universe` to read `asset_universe()` and remove the need for `coin_metadata()` fallback.
5. Add a daily refresh step after price ingestion: `REFRESH MATERIALIZED VIEW CONCURRENTLY agent_asset_universe_features`.
6. Add `scripts/agent_invest_scripts/rank_universe.py`.
7. Register `rank_universe` in `src/agent/prompt.ts`.
8. Keep `DatasetNotFoundError` only if a compatibility fallback to parquet is retained; otherwise replace it with a DB-specific error type.
9. Update tests for `list_universe`, `rank_universe`, and `run_backtest` to mock DB-backed DataFrames instead of parquet files where possible.
10. Add integration tests behind an environment flag for real Postgres views.
11. Update `src/agent/prompt.ts` tool descriptions.
12. Remove or deprecate parquet ingestion scripts once DB-backed ingestion is confirmed complete.

## Testing Plan

### Unit Tests

- `daily_prices()` returns `date`, `coin_id`, and `price`.
- `daily_prices()` returns no volume or OHLC columns.
- `asset_universe()` returns `coin_id`, `symbol`, `name`, `market_cap_rank`, `market_cap`, and `price`.
- `asset_universe_features()` returns the expected feature columns and preserves one row per `coin_id`.
- Feature values are nullable when insufficient history exists.
- `list_universe` ranks by `market_cap_rank` first and can tolerate nullable `market_cap`.
- `rank_universe` filters by market-cap rank, data history, positive trend, and max volatility.
- `rank_universe` sorts correctly by `market_cap_rank`, `momentum_180d`, `sharpe_180d`, and `low_volatility`.
- `run_backtest` still accepts allocations keyed by CoinGecko IDs.

### Integration Tests

Use a test Postgres database with a small fixture:

- Bitcoin asset
- Ethereum asset
- CoinGecko mappings
- Two days of close prices
- Latest market-cap rank on each asset

Validate:

- `list_universe --top-n 2` returns the expected rank order.
- `rank_universe --top-n 2 --positive-trend-only --sort sharpe_180d` returns the expected feature-ranked candidates.
- `run_backtest` can run a BTC/ETH allocation using the DB-backed `daily_prices()`.
- The joined `agent_asset_universe` view returns asset identity, latest rank, and latest close price.
- The materialized feature view can be refreshed after inserting new daily close prices.

## Open Decisions

1. Price source preference: should `agent_daily_close_prices` use all price sources, only `gmx`, or prefer one source when duplicates exist?
2. Coin ID policy: should every row require a CoinGecko mapping, or should the DB reader fall back to `assets.source_asset_id`?
3. Missing price policy: should universe rows without a matching close price be included for listing but excluded from backtests?
4. Market-cap value source: is `assets.metadata->>'market_cap'` populated, or should the universe expose only `market_cap_rank`?
5. Refresh scheduling: should the daily materialized-view refresh run inside the ingestion job or as a separate scheduled maintenance step?

## Recommended Defaults

- Use `assets.market_cap_rank` for ranking because only latest market-cap state is available.
- Emit CoinGecko IDs as `coin_id` whenever a mapping exists.
- Use close prices only; do not expose volume or OHLC columns in the agent dataset.
- Keep universe rows even if price is missing, because `list_universe` only needs rank and identity.
- Exclude rows without close price from `daily_prices`, because `run_backtest` requires valid prices.
- Prefer creating SQL views instead of embedding complex joins directly in Python.
- Use a materialized view for feature calculations and refresh it once per day after close-price ingestion.
