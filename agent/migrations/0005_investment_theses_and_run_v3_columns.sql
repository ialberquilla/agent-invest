CREATE EXTENSION IF NOT EXISTS "pgcrypto";
--> statement-breakpoint
CREATE TABLE "investment_theses" (
	"thesis_id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"run_id" text NOT NULL,
	"objective" text NOT NULL,
	"payload" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "investment_theses_run_id_unique" UNIQUE("run_id")
);
--> statement-breakpoint
ALTER TABLE "investment_theses" ADD CONSTRAINT "investment_theses_run_id_runs_run_id_fk" FOREIGN KEY ("run_id") REFERENCES "public"."runs"("run_id") ON DELETE cascade ON UPDATE no action;
--> statement-breakpoint
CREATE INDEX "idx_theses_run" ON "investment_theses" USING btree ("run_id");
--> statement-breakpoint
CREATE INDEX "idx_theses_objective" ON "investment_theses" USING btree ("objective");
--> statement-breakpoint
ALTER TABLE "runs" ADD COLUMN "winner_template_id" text;
--> statement-breakpoint
ALTER TABLE "runs" ADD COLUMN "winners_by_dimension" jsonb;
--> statement-breakpoint
ALTER TABLE "runs" ADD COLUMN "round_history" jsonb;
--> statement-breakpoint
ALTER TABLE "runs" ADD COLUMN "refinement_reasons" jsonb;
