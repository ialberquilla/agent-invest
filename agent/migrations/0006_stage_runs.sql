CREATE TABLE "stage_runs" (
	"stage_run_id" text PRIMARY KEY NOT NULL,
	"run_id" text NOT NULL,
	"stage" text NOT NULL,
	"round" smallint NOT NULL,
	"status" text NOT NULL,
	"opencode_session_id" text,
	"model" text NOT NULL,
	"input" jsonb NOT NULL,
	"output" jsonb,
	"error" text,
	"started_at" timestamp with time zone DEFAULT now() NOT NULL,
	"ended_at" timestamp with time zone,
	"tokens_in" integer,
	"tokens_out" integer,
	CONSTRAINT "stage_runs_stage_check" CHECK ("stage" IN ('thesis','designer','adjudicator','reporter')),
	CONSTRAINT "stage_runs_round_check" CHECK ("round" BETWEEN 1 AND 3)
);
--> statement-breakpoint
ALTER TABLE "stage_runs" ADD CONSTRAINT "stage_runs_run_id_runs_run_id_fk" FOREIGN KEY ("run_id") REFERENCES "public"."runs"("run_id") ON DELETE cascade ON UPDATE no action;
--> statement-breakpoint
CREATE INDEX "stage_runs_run_id_idx" ON "stage_runs" USING btree ("run_id", "stage", "round");
--> statement-breakpoint
CREATE INDEX "stage_runs_status_idx" ON "stage_runs" USING btree ("status");
