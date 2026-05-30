-- Repoint agent_asset_universe.market_cap from asset_prices.market_cap
-- (never populated -- the GMX price ingestion does not carry market caps)
-- to the latest snapshot in asset_market_caps (populated by the
-- coingecko-market-caps ingestion). Without this the column is NULL for
-- every asset, so any min_market_cap floor empties the universe and
-- rank_universe.py errors -- breaking the wizard's "Minimum market cap"
-- option for every value but "none". market_cap_rank is left sourced
-- from assets (unchanged) so existing rank ordering is preserved.
CREATE OR REPLACE VIEW "agent_asset_universe" AS
WITH latest_prices AS (
  SELECT DISTINCT ON ("asset_id")
    "asset_id",
    "close" AS "price",
    "timestamp" AS "latest_price_at"
  FROM "asset_prices"
  WHERE "close" IS NOT NULL
  ORDER BY "asset_id", "timestamp" DESC, "source"
), latest_market_caps AS (
  SELECT DISTINCT ON ("asset_id")
    "asset_id",
    "market_cap"
  FROM "asset_market_caps"
  ORDER BY "asset_id", "timestamp" DESC
), coingecko_mappings AS (
  SELECT "asset_id", "source_asset_id"
  FROM "asset_source_mappings"
  WHERE "source" = 'coingecko'
)
SELECT
  "assets"."asset_id",
  COALESCE("coingecko_mappings"."source_asset_id", "assets"."source_asset_id") AS "coin_id",
  "assets"."symbol",
  "assets"."name",
  "assets"."market_cap_rank",
  "latest_market_caps"."market_cap",
  "latest_prices"."price",
  "latest_prices"."latest_price_at"
FROM "assets"
LEFT JOIN "coingecko_mappings" ON "coingecko_mappings"."asset_id" = "assets"."asset_id"
LEFT JOIN "latest_prices" ON "latest_prices"."asset_id" = "assets"."asset_id"
LEFT JOIN "latest_market_caps" ON "latest_market_caps"."asset_id" = "assets"."asset_id";
--> statement-breakpoint
REFRESH MATERIALIZED VIEW "agent_asset_universe_features";
