ALTER TABLE "runs" ADD COLUMN IF NOT EXISTS "winner_template_id" text;
--> statement-breakpoint
ALTER TABLE "runs" ADD COLUMN IF NOT EXISTS "winners_by_dimension" jsonb;
--> statement-breakpoint
ALTER TABLE "runs" ADD COLUMN IF NOT EXISTS "round_history" jsonb;
--> statement-breakpoint
ALTER TABLE "runs" ADD COLUMN IF NOT EXISTS "refinement_reasons" jsonb;
--> statement-breakpoint
ALTER TABLE "runs" ADD COLUMN IF NOT EXISTS "metadata" jsonb DEFAULT '{}'::jsonb NOT NULL;
