CREATE TABLE "asset_market_caps" (
	"asset_id" text NOT NULL,
	"timestamp" timestamp with time zone NOT NULL,
	"source" text NOT NULL,
	"market_cap" numeric NOT NULL,
	"market_cap_rank" integer,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
	CONSTRAINT "asset_market_caps_asset_id_timestamp_source_pk" PRIMARY KEY("asset_id","timestamp","source")
);
--> statement-breakpoint
CREATE TABLE "asset_source_mappings" (
	"asset_id" text NOT NULL,
	"source" text NOT NULL,
	"source_asset_id" text NOT NULL,
	"confidence" text,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "asset_source_mappings_asset_id_source_pk" PRIMARY KEY("asset_id","source")
);
--> statement-breakpoint
ALTER TABLE "asset_market_caps" ADD CONSTRAINT "asset_market_caps_asset_id_assets_asset_id_fk" FOREIGN KEY ("asset_id") REFERENCES "public"."assets"("asset_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "asset_source_mappings" ADD CONSTRAINT "asset_source_mappings_asset_id_assets_asset_id_fk" FOREIGN KEY ("asset_id") REFERENCES "public"."assets"("asset_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "asset_market_caps_source_timestamp_idx" ON "asset_market_caps" USING btree ("source","timestamp");--> statement-breakpoint
CREATE INDEX "asset_market_caps_asset_id_timestamp_idx" ON "asset_market_caps" USING btree ("asset_id","timestamp");--> statement-breakpoint
CREATE INDEX "asset_market_caps_market_cap_rank_idx" ON "asset_market_caps" USING btree ("market_cap_rank");--> statement-breakpoint
CREATE UNIQUE INDEX "asset_source_mappings_source_asset_id_idx" ON "asset_source_mappings" USING btree ("source","source_asset_id");