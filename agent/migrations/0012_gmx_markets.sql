CREATE TABLE "gmx_tokens" (
	"symbol" text PRIMARY KEY NOT NULL,
	"address" text NOT NULL,
	"decimals" integer NOT NULL,
	"synthetic" boolean NOT NULL DEFAULT false,
	"updated_at" timestamp with time zone NOT NULL DEFAULT now()
);
--> statement-breakpoint
CREATE TABLE "gmx_markets" (
	"market_token" text PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"index_token" text NOT NULL,
	"long_token" text NOT NULL,
	"short_token" text NOT NULL,
	"is_listed" boolean NOT NULL DEFAULT true,
	"listing_date" timestamp with time zone,
	"updated_at" timestamp with time zone NOT NULL DEFAULT now()
);
--> statement-breakpoint
CREATE INDEX "gmx_markets_index_token_idx" ON "gmx_markets" USING btree ("index_token");
