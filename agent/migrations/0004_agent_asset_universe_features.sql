CREATE MATERIALIZED VIEW "agent_asset_universe_features" AS
WITH prices AS (
  SELECT
    p."date",
    p."coin_id",
    p."price",
    lag(p."price") OVER (PARTITION BY p."coin_id" ORDER BY p."date") AS "previous_price"
  FROM "agent_daily_close_prices" p
), returns AS (
  SELECT
    "date",
    "coin_id",
    "price",
    CASE
      WHEN "previous_price" IS NULL OR "previous_price" = 0 THEN NULL
      ELSE ("price" / "previous_price") - 1
    END AS "daily_return"
  FROM prices
), latest AS (
  SELECT DISTINCT ON ("coin_id")
    "coin_id",
    "date" AS "last_price_date",
    "price" AS "latest_price"
  FROM prices
  ORDER BY "coin_id", "date" DESC
), history AS (
  SELECT
    p."coin_id",
    min(p."date") AS "first_price_date",
    count(*) FILTER (WHERE p."date" >= l."last_price_date" - INTERVAL '365 days') AS "data_days_365d",
    avg(p."price") FILTER (WHERE p."date" >= l."last_price_date" - INTERVAL '200 days') AS "sma_200d",
    stddev_samp(r."daily_return") FILTER (WHERE r."date" > l."last_price_date" - INTERVAL '30 days') * sqrt(365) AS "volatility_30d",
    stddev_samp(r."daily_return") FILTER (WHERE r."date" > l."last_price_date" - INTERVAL '90 days') * sqrt(365) AS "volatility_90d",
    stddev_samp(r."daily_return") FILTER (WHERE r."date" > l."last_price_date" - INTERVAL '180 days') * sqrt(365) AS "volatility_180d",
    avg(r."daily_return") FILTER (WHERE r."date" > l."last_price_date" - INTERVAL '180 days') * 365 AS "annualized_return_180d"
  FROM prices p
  JOIN latest l ON l."coin_id" = p."coin_id"
  LEFT JOIN returns r
    ON r."coin_id" = p."coin_id"
   AND r."date" = p."date"
  GROUP BY p."coin_id"
), lookback_prices AS (
  SELECT
    l."coin_id",
    p30."price" AS "price_30d",
    p90."price" AS "price_90d",
    p180."price" AS "price_180d",
    p365."price" AS "price_365d"
  FROM latest l
  LEFT JOIN LATERAL (
    SELECT p."price"
    FROM prices p
    WHERE p."coin_id" = l."coin_id" AND p."date" <= l."last_price_date" - INTERVAL '30 days'
    ORDER BY p."date" DESC
    LIMIT 1
  ) p30 ON true
  LEFT JOIN LATERAL (
    SELECT p."price"
    FROM prices p
    WHERE p."coin_id" = l."coin_id" AND p."date" <= l."last_price_date" - INTERVAL '90 days'
    ORDER BY p."date" DESC
    LIMIT 1
  ) p90 ON true
  LEFT JOIN LATERAL (
    SELECT p."price"
    FROM prices p
    WHERE p."coin_id" = l."coin_id" AND p."date" <= l."last_price_date" - INTERVAL '180 days'
    ORDER BY p."date" DESC
    LIMIT 1
  ) p180 ON true
  LEFT JOIN LATERAL (
    SELECT p."price"
    FROM prices p
    WHERE p."coin_id" = l."coin_id" AND p."date" <= l."last_price_date" - INTERVAL '365 days'
    ORDER BY p."date" DESC
    LIMIT 1
  ) p365 ON true
), drawdowns AS (
  SELECT
    "coin_id",
    min("drawdown") AS "max_drawdown_180d"
  FROM (
    SELECT
      p."coin_id",
      p."date",
      (p."price" / max(p."price") OVER (
        PARTITION BY p."coin_id"
        ORDER BY p."date"
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )) - 1 AS "drawdown"
    FROM prices p
    JOIN latest l ON l."coin_id" = p."coin_id"
    WHERE p."date" > l."last_price_date" - INTERVAL '180 days'
  ) rolling
  GROUP BY "coin_id"
)
SELECT
  u."asset_id",
  u."coin_id",
  u."symbol",
  u."name",
  u."market_cap_rank",
  u."market_cap",
  l."latest_price",
  h."first_price_date",
  l."last_price_date",
  h."data_days_365d",
  CASE WHEN lp."price_30d" IS NULL OR lp."price_30d" = 0 THEN NULL ELSE (l."latest_price" / lp."price_30d") - 1 END AS "return_30d",
  CASE WHEN lp."price_90d" IS NULL OR lp."price_90d" = 0 THEN NULL ELSE (l."latest_price" / lp."price_90d") - 1 END AS "return_90d",
  CASE WHEN lp."price_180d" IS NULL OR lp."price_180d" = 0 THEN NULL ELSE (l."latest_price" / lp."price_180d") - 1 END AS "return_180d",
  CASE WHEN lp."price_365d" IS NULL OR lp."price_365d" = 0 THEN NULL ELSE (l."latest_price" / lp."price_365d") - 1 END AS "return_365d",
  h."volatility_30d",
  h."volatility_90d",
  h."volatility_180d",
  d."max_drawdown_180d",
  CASE WHEN h."volatility_180d" IS NULL OR h."volatility_180d" = 0 THEN NULL ELSE h."annualized_return_180d" / h."volatility_180d" END AS "sharpe_180d",
  CASE WHEN h."sma_200d" IS NULL THEN NULL ELSE l."latest_price" > h."sma_200d" END AS "price_above_sma_200d"
FROM "agent_asset_universe" u
LEFT JOIN latest l ON l."coin_id" = u."coin_id"
LEFT JOIN history h ON h."coin_id" = u."coin_id"
LEFT JOIN lookback_prices lp ON lp."coin_id" = u."coin_id"
LEFT JOIN drawdowns d ON d."coin_id" = u."coin_id";
--> statement-breakpoint
CREATE UNIQUE INDEX "agent_asset_universe_features_coin_id_idx"
  ON "agent_asset_universe_features" ("coin_id");
--> statement-breakpoint
CREATE INDEX "agent_asset_universe_features_rank_idx"
  ON "agent_asset_universe_features" ("market_cap_rank");
