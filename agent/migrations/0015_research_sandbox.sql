CREATE VIEW "agent_daily_ohlc" AS
WITH coingecko_mappings AS (
  SELECT
    "asset_id",
    "source_asset_id"
  FROM "asset_source_mappings"
  WHERE "source" = 'coingecko'
), ranked_daily_prices AS (
  SELECT
    "asset_prices"."timestamp"::date AS "date",
    "asset_prices"."asset_id",
    COALESCE(coingecko_mappings."source_asset_id", "assets"."source_asset_id") AS "coin_id",
    "asset_prices"."open",
    "asset_prices"."high",
    "asset_prices"."low",
    "asset_prices"."close",
    "asset_prices"."volume",
    "asset_prices"."timestamp",
    "asset_prices"."source",
    row_number() OVER (
      PARTITION BY
        "asset_prices"."timestamp"::date,
        COALESCE(coingecko_mappings."source_asset_id", "assets"."source_asset_id")
      ORDER BY "asset_prices"."timestamp" DESC, "asset_prices"."source" ASC, "asset_prices"."asset_id" ASC
    ) AS "price_rank"
  FROM "asset_prices"
  JOIN "assets" ON "assets"."asset_id" = "asset_prices"."asset_id"
  LEFT JOIN coingecko_mappings ON coingecko_mappings."asset_id" = "assets"."asset_id"
  WHERE "asset_prices"."close" IS NOT NULL
)
SELECT
  "date",
  "asset_id",
  "coin_id",
  "open",
  "high",
  "low",
  "close",
  "volume",
  "timestamp",
  "source"
FROM ranked_daily_prices
WHERE "price_rank" = 1;
--> statement-breakpoint
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agent_readonly') THEN
    CREATE ROLE agent_readonly LOGIN;
  END IF;
END
$$;
--> statement-breakpoint
ALTER ROLE agent_readonly SET statement_timeout = '10s';
--> statement-breakpoint
ALTER ROLE agent_readonly SET default_transaction_read_only = on;
--> statement-breakpoint
GRANT USAGE ON SCHEMA public TO agent_readonly;
--> statement-breakpoint
GRANT SELECT ON TABLE
  "asset_prices",
  "agent_daily_close_prices",
  "agent_daily_ohlc",
  "agent_asset_universe",
  "agent_asset_universe_features"
TO agent_readonly;
