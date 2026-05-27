CREATE TABLE "stage_eval_runs" (
	"eval_run_id" text PRIMARY KEY NOT NULL,
	"stage" text NOT NULL,
	"fixture_id" text NOT NULL,
	"model" text NOT NULL,
	"passed" boolean NOT NULL,
	"score" real,
	"diagnostics" jsonb NOT NULL,
	"output" jsonb NOT NULL,
	"duration_ms" integer NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "stage_eval_runs_stage_check" CHECK ("stage" in ('thesis','designer','adjudicator','reporter'))
);
--> statement-breakpoint
CREATE INDEX "stage_eval_runs_fixture_idx" ON "stage_eval_runs" USING btree ("stage","fixture_id","created_at" DESC);
