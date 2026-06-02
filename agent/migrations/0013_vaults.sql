CREATE TABLE "vaults" (
	"chain_id" integer NOT NULL,
	"vault_address" text NOT NULL,
	"mandate_id" text NOT NULL,
	"asset_address" text NOT NULL,
	"status" text NOT NULL DEFAULT 'active',
	"created_at" timestamp with time zone NOT NULL DEFAULT now(),
	"updated_at" timestamp with time zone NOT NULL DEFAULT now(),
	CONSTRAINT "vaults_chain_id_vault_address_pk" PRIMARY KEY("chain_id","vault_address")
);
--> statement-breakpoint
ALTER TABLE "vaults" ADD CONSTRAINT "vaults_mandate_id_strategy_mandates_mandate_id_fk" FOREIGN KEY ("mandate_id") REFERENCES "public"."strategy_mandates"("mandate_id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "vaults_mandate_id_idx" ON "vaults" USING btree ("mandate_id");--> statement-breakpoint
CREATE INDEX "vaults_status_idx" ON "vaults" USING btree ("status");
