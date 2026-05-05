import { desc, sql } from "drizzle-orm";
import {
  bigint,
  check,
  date,
  index,
  integer,
  jsonb,
  numeric,
  pgTable,
  primaryKey,
  text,
  timestamp,
  uniqueIndex,
} from "drizzle-orm/pg-core";

export const users = pgTable("users", {
  userId: text("user_id").primaryKey(),
  createdAt: timestamp("created_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
});

export const strategies = pgTable(
  "strategies",
  {
    strategyId: text("strategy_id").primaryKey(),
    userId: text("user_id")
      .notNull()
      .references(() => users.userId, { onDelete: "cascade" }),
    opencodeSessionId: text("opencode_session_id"),
    title: text("title").notNull().default(""),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    lastUsedAt: timestamp("last_used_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    index("strategies_user_id_last_used_at_idx").on(
      table.userId,
      desc(table.lastUsedAt),
    ),
    index("strategies_opencode_session_id_idx").on(table.opencodeSessionId),
  ],
);

export const conversationThreads = pgTable(
  "conversation_threads",
  {
    threadId: text("thread_id").primaryKey(),
    userId: text("user_id")
      .notNull()
      .references(() => users.userId, { onDelete: "cascade" }),
    strategyId: text("strategy_id").references(() => strategies.strategyId, {
      onDelete: "cascade",
    }),
    provider: text("provider").notNull().default("opencode"),
    providerSessionId: text("provider_session_id"),
    title: text("title").notNull().default(""),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    index("conversation_threads_user_id_updated_at_idx").on(
      table.userId,
      desc(table.updatedAt),
    ),
    index("conversation_threads_strategy_id_updated_at_idx").on(
      table.strategyId,
      desc(table.updatedAt),
    ),
    index("conversation_threads_provider_session_idx").on(
      table.provider,
      table.providerSessionId,
    ),
  ],
);

export const conversationMessages = pgTable(
  "conversation_messages",
  {
    messageId: text("message_id").primaryKey(),
    threadId: text("thread_id")
      .notNull()
      .references(() => conversationThreads.threadId, { onDelete: "cascade" }),
    role: text("role").notNull(),
    content: text("content").notNull().default(""),
    model: text("model"),
    providerMessageId: text("provider_message_id"),
    status: text("status").notNull().default("completed"),
    tokenInput: integer("token_input"),
    tokenOutput: integer("token_output"),
    metadata: jsonb("metadata").notNull().default({}),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    index("conversation_messages_thread_id_created_at_idx").on(
      table.threadId,
      table.createdAt,
    ),
    index("conversation_messages_role_idx").on(table.role),
    index("conversation_messages_created_at_idx").on(desc(table.createdAt)),
    check(
      "conversation_messages_role_check",
      sql`${table.role} in ('system', 'user', 'assistant', 'tool')`,
    ),
  ],
);

export const runs = pgTable(
  "runs",
  {
    runId: text("run_id").primaryKey(),
    strategyId: text("strategy_id").references(() => strategies.strategyId, {
      onDelete: "cascade",
    }),
    threadId: text("thread_id").references(() => conversationThreads.threadId, {
      onDelete: "set null",
    }),
    kind: text("kind").notNull().default("agent_turn"),
    status: text("status").notNull(),
    startedAt: timestamp("started_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    endedAt: timestamp("ended_at", { withTimezone: true }),
    exitCode: integer("exit_code"),
    reply: text("reply"),
    error: text("error"),
    metadata: jsonb("metadata").notNull().default({}),
  },
  (table) => [
    index("runs_strategy_id_started_at_idx").on(
      table.strategyId,
      desc(table.startedAt),
    ),
    index("runs_thread_id_started_at_idx").on(
      table.threadId,
      desc(table.startedAt),
    ),
  ],
);

export const agentEvents = pgTable(
  "agent_events",
  {
    eventId: text("event_id").primaryKey(),
    threadId: text("thread_id").references(() => conversationThreads.threadId, {
      onDelete: "cascade",
    }),
    messageId: text("message_id").references(
      () => conversationMessages.messageId,
      { onDelete: "set null" },
    ),
    runId: text("run_id").references(() => runs.runId, {
      onDelete: "set null",
    }),
    eventType: text("event_type").notNull(),
    payload: jsonb("payload").notNull().default({}),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    index("agent_events_thread_id_created_at_idx").on(
      table.threadId,
      table.createdAt,
    ),
    index("agent_events_message_id_created_at_idx").on(
      table.messageId,
      table.createdAt,
    ),
    index("agent_events_run_id_created_at_idx").on(
      table.runId,
      table.createdAt,
    ),
    index("agent_events_event_type_created_at_idx").on(
      table.eventType,
      desc(table.createdAt),
    ),
  ],
);

export const backtestRequests = pgTable(
  "backtest_requests",
  {
    backtestId: text("backtest_id").primaryKey(),
    runId: text("run_id")
      .notNull()
      .references(() => runs.runId, { onDelete: "cascade" }),
    strategyId: text("strategy_id").references(() => strategies.strategyId, {
      onDelete: "cascade",
    }),
    allocation: jsonb("allocation").notNull(),
    rebalance: text("rebalance").notNull().default("none"),
    costs: jsonb("costs").notNull().default({}),
    initialCapitalUsd: numeric("initial_capital_usd"),
    startDate: date("start_date"),
    endDate: date("end_date"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    index("backtest_requests_run_id_idx").on(table.runId),
    index("backtest_requests_strategy_id_created_at_idx").on(
      table.strategyId,
      desc(table.createdAt),
    ),
    index("backtest_requests_start_date_end_date_idx").on(
      table.startDate,
      table.endDate,
    ),
  ],
);

export const backtestResults = pgTable(
  "backtest_results",
  {
    backtestId: text("backtest_id")
      .primaryKey()
      .references(() => backtestRequests.backtestId, { onDelete: "cascade" }),
    runId: text("run_id")
      .notNull()
      .references(() => runs.runId, { onDelete: "cascade" }),
    cagr: numeric("cagr"),
    sharpeRatio: numeric("sharpe_ratio"),
    sortinoRatio: numeric("sortino_ratio"),
    maxDrawdown: numeric("max_drawdown"),
    calmarRatio: numeric("calmar_ratio"),
    monthlyHitRate: numeric("monthly_hit_rate"),
    finalEquityUsd: numeric("final_equity_usd"),
    totalTradingCostUsd: numeric("total_trading_cost_usd"),
    totalNumSwaps: integer("total_num_swaps"),
    report: jsonb("report").notNull().default({}),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    index("backtest_results_run_id_idx").on(table.runId),
    index("backtest_results_created_at_idx").on(desc(table.createdAt)),
    index("backtest_results_sharpe_ratio_idx").on(desc(table.sharpeRatio)),
    index("backtest_results_cagr_idx").on(desc(table.cagr)),
    index("backtest_results_max_drawdown_idx").on(table.maxDrawdown),
  ],
);

export const assets = pgTable(
  "assets",
  {
    assetId: text("asset_id").primaryKey(),
    source: text("source").notNull(),
    sourceAssetId: text("source_asset_id").notNull(),
    symbol: text("symbol").notNull(),
    name: text("name").notNull(),
    marketCapRank: integer("market_cap_rank"),
    metadata: jsonb("metadata").notNull().default({}),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    uniqueIndex("assets_source_asset_id_idx").on(
      table.source,
      table.sourceAssetId,
    ),
    index("assets_symbol_idx").on(table.symbol),
    index("assets_market_cap_rank_idx").on(table.marketCapRank),
  ],
);

export const assetPrices = pgTable(
  "asset_prices",
  {
    assetId: text("asset_id")
      .notNull()
      .references(() => assets.assetId, { onDelete: "cascade" }),
    timestamp: timestamp("timestamp", { withTimezone: true }).notNull(),
    source: text("source").notNull(),
    open: numeric("open"),
    high: numeric("high"),
    low: numeric("low"),
    close: numeric("close").notNull(),
    volume: numeric("volume"),
    marketCap: numeric("market_cap"),
    metadata: jsonb("metadata").notNull().default({}),
  },
  (table) => [
    primaryKey({
      name: "asset_prices_asset_id_timestamp_source_pk",
      columns: [table.assetId, table.timestamp, table.source],
    }),
    index("asset_prices_asset_id_timestamp_idx").on(
      table.assetId,
      table.timestamp,
    ),
    index("asset_prices_timestamp_idx").on(table.timestamp),
    index("asset_prices_source_timestamp_idx").on(
      table.source,
      table.timestamp,
    ),
  ],
);

export const assetSourceMappings = pgTable(
  "asset_source_mappings",
  {
    assetId: text("asset_id")
      .notNull()
      .references(() => assets.assetId, { onDelete: "cascade" }),
    source: text("source").notNull(),
    sourceAssetId: text("source_asset_id").notNull(),
    confidence: text("confidence"),
    metadata: jsonb("metadata").notNull().default({}),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    primaryKey({
      name: "asset_source_mappings_asset_id_source_pk",
      columns: [table.assetId, table.source],
    }),
    uniqueIndex("asset_source_mappings_source_asset_id_idx").on(
      table.source,
      table.sourceAssetId,
    ),
  ],
);

export const assetMarketCaps = pgTable(
  "asset_market_caps",
  {
    assetId: text("asset_id")
      .notNull()
      .references(() => assets.assetId, { onDelete: "cascade" }),
    timestamp: timestamp("timestamp", { withTimezone: true }).notNull(),
    source: text("source").notNull(),
    marketCap: numeric("market_cap").notNull(),
    marketCapRank: integer("market_cap_rank"),
    metadata: jsonb("metadata").notNull().default({}),
  },
  (table) => [
    primaryKey({
      name: "asset_market_caps_asset_id_timestamp_source_pk",
      columns: [table.assetId, table.timestamp, table.source],
    }),
    index("asset_market_caps_source_timestamp_idx").on(
      table.source,
      table.timestamp,
    ),
    index("asset_market_caps_asset_id_timestamp_idx").on(
      table.assetId,
      table.timestamp,
    ),
    index("asset_market_caps_market_cap_rank_idx").on(table.marketCapRank),
  ],
);

export const artifacts = pgTable(
  "artifacts",
  {
    artifactId: text("artifact_id").primaryKey(),
    runId: text("run_id").references(() => runs.runId, { onDelete: "cascade" }),
    backtestId: text("backtest_id").references(
      () => backtestRequests.backtestId,
      { onDelete: "cascade" },
    ),
    kind: text("kind").notNull(),
    storageKey: text("storage_key").notNull(),
    contentType: text("content_type").notNull(),
    sizeBytes: bigint("size_bytes", { mode: "number" }),
    checksum: text("checksum"),
    metadata: jsonb("metadata").notNull().default({}),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    index("artifacts_run_id_created_at_idx").on(table.runId, table.createdAt),
    index("artifacts_backtest_id_created_at_idx").on(
      table.backtestId,
      table.createdAt,
    ),
    index("artifacts_kind_idx").on(table.kind),
    uniqueIndex("artifacts_storage_key_idx").on(table.storageKey),
  ],
);

export type User = typeof users.$inferSelect;
export type NewUser = typeof users.$inferInsert;

export type Strategy = typeof strategies.$inferSelect;
export type NewStrategy = typeof strategies.$inferInsert;

export type ConversationThread = typeof conversationThreads.$inferSelect;
export type NewConversationThread = typeof conversationThreads.$inferInsert;

export type ConversationMessage = typeof conversationMessages.$inferSelect;
export type NewConversationMessage = typeof conversationMessages.$inferInsert;

export type Run = typeof runs.$inferSelect;
export type NewRun = typeof runs.$inferInsert;

export type AgentEvent = typeof agentEvents.$inferSelect;
export type NewAgentEvent = typeof agentEvents.$inferInsert;

export type BacktestRequest = typeof backtestRequests.$inferSelect;
export type NewBacktestRequest = typeof backtestRequests.$inferInsert;

export type BacktestResult = typeof backtestResults.$inferSelect;
export type NewBacktestResult = typeof backtestResults.$inferInsert;

export type Asset = typeof assets.$inferSelect;
export type NewAsset = typeof assets.$inferInsert;

export type AssetPrice = typeof assetPrices.$inferSelect;
export type NewAssetPrice = typeof assetPrices.$inferInsert;

export type AssetSourceMapping = typeof assetSourceMappings.$inferSelect;
export type NewAssetSourceMapping = typeof assetSourceMappings.$inferInsert;

export type AssetMarketCap = typeof assetMarketCaps.$inferSelect;
export type NewAssetMarketCap = typeof assetMarketCaps.$inferInsert;

export type Artifact = typeof artifacts.$inferSelect;
export type NewArtifact = typeof artifacts.$inferInsert;
