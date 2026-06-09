CREATE TABLE "strategy_mandates" (
	"mandate_id" text PRIMARY KEY NOT NULL,
	"run_id" text NOT NULL,
	"version" integer NOT NULL DEFAULT 1,
	"status" text NOT NULL DEFAULT 'pending',
	"template_id" text NOT NULL,
	"spec" jsonb NOT NULL,
	"created_at" timestamp with time zone NOT NULL DEFAULT now(),
	"updated_at" timestamp with time zone NOT NULL DEFAULT now()
);
--> statement-breakpoint
ALTER TABLE "strategy_mandates" ADD CONSTRAINT "strategy_mandates_run_id_runs_run_id_fk" FOREIGN KEY ("run_id") REFERENCES "public"."runs"("run_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "strategy_mandates_run_id_idx" ON "strategy_mandates" USING btree ("run_id");--> statement-breakpoint
CREATE INDEX "strategy_mandates_status_idx" ON "strategy_mandates" USING btree ("status");
