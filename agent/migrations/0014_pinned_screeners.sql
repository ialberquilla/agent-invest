CREATE TABLE "pinned_screeners" (
	"screener_id" text NOT NULL,
	"user_id" text NOT NULL,
	"title" text NOT NULL,
	"definition" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	"last_refreshed_at" timestamp with time zone,
	CONSTRAINT "pinned_screeners_user_id_screener_id_pk" PRIMARY KEY("user_id","screener_id")
);
--> statement-breakpoint
ALTER TABLE "pinned_screeners" ADD CONSTRAINT "pinned_screeners_user_id_users_user_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users"("user_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "pinned_screeners_user_id_updated_at_idx" ON "pinned_screeners" USING btree ("user_id","updated_at" DESC);
