DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS "pgcrypto";
EXCEPTION
  WHEN insufficient_privilege OR feature_not_supported THEN
    RAISE NOTICE 'Skipping pgcrypto extension setup: %', SQLERRM;
END
$$;
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "investment_theses" (
	"thesis_id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"run_id" text NOT NULL,
	"objective" text NOT NULL,
	"payload" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "investment_theses_run_id_unique" UNIQUE("run_id")
);
--> statement-breakpoint
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'investment_theses_run_id_runs_run_id_fk'
      AND conrelid = 'public.investment_theses'::regclass
  ) THEN
    ALTER TABLE "investment_theses" ADD CONSTRAINT "investment_theses_run_id_runs_run_id_fk" FOREIGN KEY ("run_id") REFERENCES "public"."runs"("run_id") ON DELETE cascade ON UPDATE no action;
  END IF;
END
$$;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "idx_theses_run" ON "investment_theses" USING btree ("run_id");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "idx_theses_objective" ON "investment_theses" USING btree ("objective");
--> statement-breakpoint
ALTER TABLE "runs" ADD COLUMN IF NOT EXISTS "winner_template_id" text;
--> statement-breakpoint
ALTER TABLE "runs" ADD COLUMN IF NOT EXISTS "winners_by_dimension" jsonb;
--> statement-breakpoint
ALTER TABLE "runs" ADD COLUMN IF NOT EXISTS "round_history" jsonb;
--> statement-breakpoint
ALTER TABLE "runs" ADD COLUMN IF NOT EXISTS "refinement_reasons" jsonb;
