CREATE TABLE "agent_events" (
	"event_id" text PRIMARY KEY NOT NULL,
	"thread_id" text,
	"message_id" text,
	"run_id" text,
	"event_type" text NOT NULL,
	"payload" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "artifacts" (
	"artifact_id" text PRIMARY KEY NOT NULL,
	"run_id" text,
	"backtest_id" text,
	"kind" text NOT NULL,
	"storage_key" text NOT NULL,
	"content_type" text NOT NULL,
	"size_bytes" bigint,
	"checksum" text,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "asset_prices" (
	"asset_id" text NOT NULL,
	"timestamp" timestamp with time zone NOT NULL,
	"source" text NOT NULL,
	"open" numeric,
	"high" numeric,
	"low" numeric,
	"close" numeric NOT NULL,
	"volume" numeric,
	"market_cap" numeric,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
	CONSTRAINT "asset_prices_asset_id_timestamp_source_pk" PRIMARY KEY("asset_id","timestamp","source")
);
--> statement-breakpoint
CREATE TABLE "assets" (
	"asset_id" text PRIMARY KEY NOT NULL,
	"source" text NOT NULL,
	"source_asset_id" text NOT NULL,
	"symbol" text NOT NULL,
	"name" text NOT NULL,
	"market_cap_rank" integer,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "backtest_requests" (
	"backtest_id" text PRIMARY KEY NOT NULL,
	"run_id" text NOT NULL,
	"strategy_id" text,
	"allocation" jsonb NOT NULL,
	"rebalance" text DEFAULT 'none' NOT NULL,
	"costs" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"initial_capital_usd" numeric,
	"start_date" date,
	"end_date" date,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "backtest_results" (
	"backtest_id" text PRIMARY KEY NOT NULL,
	"run_id" text NOT NULL,
	"cagr" numeric,
	"sharpe_ratio" numeric,
	"sortino_ratio" numeric,
	"max_drawdown" numeric,
	"calmar_ratio" numeric,
	"monthly_hit_rate" numeric,
	"final_equity_usd" numeric,
	"total_trading_cost_usd" numeric,
	"total_num_swaps" integer,
	"report" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "conversation_messages" (
	"message_id" text PRIMARY KEY NOT NULL,
	"thread_id" text NOT NULL,
	"role" text NOT NULL,
	"content" text DEFAULT '' NOT NULL,
	"model" text,
	"provider_message_id" text,
	"status" text DEFAULT 'completed' NOT NULL,
	"token_input" integer,
	"token_output" integer,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "conversation_messages_role_check" CHECK ("conversation_messages"."role" in ('system', 'user', 'assistant', 'tool'))
);
--> statement-breakpoint
CREATE TABLE "conversation_threads" (
	"thread_id" text PRIMARY KEY NOT NULL,
	"user_id" text NOT NULL,
	"strategy_id" text,
	"provider" text DEFAULT 'opencode' NOT NULL,
	"provider_session_id" text,
	"title" text DEFAULT '' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "runs" (
	"run_id" text PRIMARY KEY NOT NULL,
	"strategy_id" text,
	"thread_id" text,
	"kind" text DEFAULT 'agent_turn' NOT NULL,
	"status" text NOT NULL,
	"started_at" timestamp with time zone DEFAULT now() NOT NULL,
	"ended_at" timestamp with time zone,
	"exit_code" integer,
	"reply" text,
	"error" text,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL
);
--> statement-breakpoint
CREATE TABLE "strategies" (
	"strategy_id" text PRIMARY KEY NOT NULL,
	"user_id" text NOT NULL,
	"opencode_session_id" text,
	"title" text DEFAULT '' NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"last_used_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "users" (
	"user_id" text PRIMARY KEY NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "agent_events" ADD CONSTRAINT "agent_events_thread_id_conversation_threads_thread_id_fk" FOREIGN KEY ("thread_id") REFERENCES "public"."conversation_threads"("thread_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_events" ADD CONSTRAINT "agent_events_message_id_conversation_messages_message_id_fk" FOREIGN KEY ("message_id") REFERENCES "public"."conversation_messages"("message_id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "agent_events" ADD CONSTRAINT "agent_events_run_id_runs_run_id_fk" FOREIGN KEY ("run_id") REFERENCES "public"."runs"("run_id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "artifacts" ADD CONSTRAINT "artifacts_run_id_runs_run_id_fk" FOREIGN KEY ("run_id") REFERENCES "public"."runs"("run_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "artifacts" ADD CONSTRAINT "artifacts_backtest_id_backtest_requests_backtest_id_fk" FOREIGN KEY ("backtest_id") REFERENCES "public"."backtest_requests"("backtest_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "asset_prices" ADD CONSTRAINT "asset_prices_asset_id_assets_asset_id_fk" FOREIGN KEY ("asset_id") REFERENCES "public"."assets"("asset_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "backtest_requests" ADD CONSTRAINT "backtest_requests_run_id_runs_run_id_fk" FOREIGN KEY ("run_id") REFERENCES "public"."runs"("run_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "backtest_requests" ADD CONSTRAINT "backtest_requests_strategy_id_strategies_strategy_id_fk" FOREIGN KEY ("strategy_id") REFERENCES "public"."strategies"("strategy_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "backtest_results" ADD CONSTRAINT "backtest_results_backtest_id_backtest_requests_backtest_id_fk" FOREIGN KEY ("backtest_id") REFERENCES "public"."backtest_requests"("backtest_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "backtest_results" ADD CONSTRAINT "backtest_results_run_id_runs_run_id_fk" FOREIGN KEY ("run_id") REFERENCES "public"."runs"("run_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "conversation_messages" ADD CONSTRAINT "conversation_messages_thread_id_conversation_threads_thread_id_fk" FOREIGN KEY ("thread_id") REFERENCES "public"."conversation_threads"("thread_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "conversation_threads" ADD CONSTRAINT "conversation_threads_user_id_users_user_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users"("user_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "conversation_threads" ADD CONSTRAINT "conversation_threads_strategy_id_strategies_strategy_id_fk" FOREIGN KEY ("strategy_id") REFERENCES "public"."strategies"("strategy_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "runs" ADD CONSTRAINT "runs_strategy_id_strategies_strategy_id_fk" FOREIGN KEY ("strategy_id") REFERENCES "public"."strategies"("strategy_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "runs" ADD CONSTRAINT "runs_thread_id_conversation_threads_thread_id_fk" FOREIGN KEY ("thread_id") REFERENCES "public"."conversation_threads"("thread_id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "strategies" ADD CONSTRAINT "strategies_user_id_users_user_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users"("user_id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "agent_events_thread_id_created_at_idx" ON "agent_events" USING btree ("thread_id","created_at");--> statement-breakpoint
CREATE INDEX "agent_events_message_id_created_at_idx" ON "agent_events" USING btree ("message_id","created_at");--> statement-breakpoint
CREATE INDEX "agent_events_run_id_created_at_idx" ON "agent_events" USING btree ("run_id","created_at");--> statement-breakpoint
CREATE INDEX "agent_events_event_type_created_at_idx" ON "agent_events" USING btree ("event_type","created_at" desc);--> statement-breakpoint
CREATE INDEX "artifacts_run_id_created_at_idx" ON "artifacts" USING btree ("run_id","created_at");--> statement-breakpoint
CREATE INDEX "artifacts_backtest_id_created_at_idx" ON "artifacts" USING btree ("backtest_id","created_at");--> statement-breakpoint
CREATE INDEX "artifacts_kind_idx" ON "artifacts" USING btree ("kind");--> statement-breakpoint
CREATE UNIQUE INDEX "artifacts_storage_key_idx" ON "artifacts" USING btree ("storage_key");--> statement-breakpoint
CREATE INDEX "asset_prices_asset_id_timestamp_idx" ON "asset_prices" USING btree ("asset_id","timestamp");--> statement-breakpoint
CREATE INDEX "asset_prices_timestamp_idx" ON "asset_prices" USING btree ("timestamp");--> statement-breakpoint
CREATE INDEX "asset_prices_source_timestamp_idx" ON "asset_prices" USING btree ("source","timestamp");--> statement-breakpoint
CREATE UNIQUE INDEX "assets_source_asset_id_idx" ON "assets" USING btree ("source","source_asset_id");--> statement-breakpoint
CREATE INDEX "assets_symbol_idx" ON "assets" USING btree ("symbol");--> statement-breakpoint
CREATE INDEX "assets_market_cap_rank_idx" ON "assets" USING btree ("market_cap_rank");--> statement-breakpoint
CREATE INDEX "backtest_requests_run_id_idx" ON "backtest_requests" USING btree ("run_id");--> statement-breakpoint
CREATE INDEX "backtest_requests_strategy_id_created_at_idx" ON "backtest_requests" USING btree ("strategy_id","created_at" desc);--> statement-breakpoint
CREATE INDEX "backtest_requests_start_date_end_date_idx" ON "backtest_requests" USING btree ("start_date","end_date");--> statement-breakpoint
CREATE INDEX "backtest_results_run_id_idx" ON "backtest_results" USING btree ("run_id");--> statement-breakpoint
CREATE INDEX "backtest_results_created_at_idx" ON "backtest_results" USING btree ("created_at" desc);--> statement-breakpoint
CREATE INDEX "backtest_results_sharpe_ratio_idx" ON "backtest_results" USING btree ("sharpe_ratio" desc);--> statement-breakpoint
CREATE INDEX "backtest_results_cagr_idx" ON "backtest_results" USING btree ("cagr" desc);--> statement-breakpoint
CREATE INDEX "backtest_results_max_drawdown_idx" ON "backtest_results" USING btree ("max_drawdown");--> statement-breakpoint
CREATE INDEX "conversation_messages_thread_id_created_at_idx" ON "conversation_messages" USING btree ("thread_id","created_at");--> statement-breakpoint
CREATE INDEX "conversation_messages_role_idx" ON "conversation_messages" USING btree ("role");--> statement-breakpoint
CREATE INDEX "conversation_messages_created_at_idx" ON "conversation_messages" USING btree ("created_at" desc);--> statement-breakpoint
CREATE INDEX "conversation_threads_user_id_updated_at_idx" ON "conversation_threads" USING btree ("user_id","updated_at" desc);--> statement-breakpoint
CREATE INDEX "conversation_threads_strategy_id_updated_at_idx" ON "conversation_threads" USING btree ("strategy_id","updated_at" desc);--> statement-breakpoint
CREATE INDEX "conversation_threads_provider_session_idx" ON "conversation_threads" USING btree ("provider","provider_session_id");--> statement-breakpoint
CREATE INDEX "runs_strategy_id_started_at_idx" ON "runs" USING btree ("strategy_id","started_at" desc);--> statement-breakpoint
CREATE INDEX "runs_thread_id_started_at_idx" ON "runs" USING btree ("thread_id","started_at" desc);--> statement-breakpoint
CREATE INDEX "strategies_user_id_last_used_at_idx" ON "strategies" USING btree ("user_id","last_used_at" desc);--> statement-breakpoint
CREATE INDEX "strategies_opencode_session_id_idx" ON "strategies" USING btree ("opencode_session_id");