CREATE TABLE "agent_tool_calls" (
	"tool_call_id" text PRIMARY KEY NOT NULL,
	"run_id" text,
	"thread_id" text,
	"stage_run_id" text,
	"tool_name" text NOT NULL,
	"args" jsonb NOT NULL DEFAULT '{}'::jsonb,
	"result" jsonb,
	"is_error" boolean NOT NULL DEFAULT false,
	"error_message" text,
	"started_at" timestamp with time zone NOT NULL DEFAULT now(),
	"finished_at" timestamp with time zone,
	"duration_ms" integer,
	"started_event_id" text,
	"finished_event_id" text,
	CONSTRAINT "agent_tool_calls_run_id_fk" FOREIGN KEY ("run_id") REFERENCES "runs"("run_id") ON DELETE cascade ON UPDATE no action,
	CONSTRAINT "agent_tool_calls_thread_id_fk" FOREIGN KEY ("thread_id") REFERENCES "conversation_threads"("thread_id") ON DELETE cascade ON UPDATE no action,
	CONSTRAINT "agent_tool_calls_stage_run_id_fk" FOREIGN KEY ("stage_run_id") REFERENCES "stage_runs"("stage_run_id") ON DELETE set null ON UPDATE no action,
	CONSTRAINT "agent_tool_calls_started_event_id_fk" FOREIGN KEY ("started_event_id") REFERENCES "agent_events"("event_id") ON DELETE set null ON UPDATE no action,
	CONSTRAINT "agent_tool_calls_finished_event_id_fk" FOREIGN KEY ("finished_event_id") REFERENCES "agent_events"("event_id") ON DELETE set null ON UPDATE no action,
	CONSTRAINT "agent_tool_calls_parent_check" CHECK ("run_id" IS NOT NULL OR "thread_id" IS NOT NULL)
);
--> statement-breakpoint
CREATE INDEX "agent_tool_calls_run_id_started_at_idx" ON "agent_tool_calls" USING btree ("run_id","started_at");
--> statement-breakpoint
CREATE INDEX "agent_tool_calls_thread_id_started_at_idx" ON "agent_tool_calls" USING btree ("thread_id","started_at");
--> statement-breakpoint
CREATE INDEX "agent_tool_calls_tool_name_idx" ON "agent_tool_calls" USING btree ("tool_name");
