CREATE VIEW "agent_asset_universe" AS
WITH latest_prices AS (
  SELECT DISTINCT ON ("asset_id")
    "asset_id",
    "close" AS "price",
    "market_cap",
    "timestamp" AS "latest_price_at"
  FROM "asset_prices"
  WHERE "close" IS NOT NULL
  ORDER BY "asset_id", "timestamp" DESC, "source" ASC
), coingecko_mappings AS (
  SELECT
    "asset_id",
    "source_asset_id"
  FROM "asset_source_mappings"
  WHERE "source" = 'coingecko'
)
SELECT
  "assets"."asset_id",
  COALESCE(coingecko_mappings."source_asset_id", "assets"."source_asset_id") AS "coin_id",
  "assets"."symbol",
  "assets"."name",
  "assets"."market_cap_rank",
  latest_prices."market_cap",
  latest_prices."price",
  latest_prices."latest_price_at"
FROM "assets"
LEFT JOIN coingecko_mappings ON coingecko_mappings."asset_id" = "assets"."asset_id"
LEFT JOIN latest_prices ON latest_prices."asset_id" = "assets"."asset_id";
--> statement-breakpoint
CREATE VIEW "agent_daily_close_prices" AS
WITH coingecko_mappings AS (
  SELECT
    "asset_id",
    "source_asset_id"
  FROM "asset_source_mappings"
  WHERE "source" = 'coingecko'
), ranked_daily_prices AS (
  SELECT
    "asset_prices"."timestamp"::date AS "date",
    COALESCE(coingecko_mappings."source_asset_id", "assets"."source_asset_id") AS "coin_id",
    "asset_prices"."close" AS "price",
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
  "coin_id",
  "price",
  "timestamp",
  "source"
FROM ranked_daily_prices
WHERE "price_rank" = 1;
