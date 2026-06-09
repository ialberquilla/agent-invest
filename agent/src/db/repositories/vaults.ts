import { and, eq } from "drizzle-orm";

import { db as defaultDb } from "../client";
import {
  type NewVaultRow,
  type VaultRow,
  strategyMandates,
  vaults,
} from "../schema";

type Db = Omit<typeof defaultDb, "$client">;

export type VaultBinding = {
  chainId: number;
  vaultAddress: string;
  mandateId: string;
  // ERC-4626 underlying (USDC).
  assetAddress: string;
};

// Persist a deployed vault row. Idempotent on (chainId, vaultAddress) so a
// retried bind never double-writes.
export async function insertVault(
  binding: VaultBinding,
  db: Db = defaultDb,
): Promise<VaultRow | null> {
  const row: NewVaultRow = {
    chainId: binding.chainId,
    vaultAddress: binding.vaultAddress,
    mandateId: binding.mandateId,
    assetAddress: binding.assetAddress,
    status: "active",
  };

  const [inserted] = await db
    .insert(vaults)
    .values(row)
    .onConflictDoNothing()
    .returning();

  return inserted ?? null;
}

// Bind a deployed vault to its mandate and promote the mandate to `active`.
// This is the one-time hook after deploy + fund: it records where the strategy
// trades and flips the mandate from `pending` to the execution-ready state.
export async function bindVaultToMandate(
  binding: VaultBinding,
  db: Db = defaultDb,
): Promise<VaultRow | null> {
  const inserted = await insertVault(binding, db);

  await db
    .update(strategyMandates)
    .set({ status: "active", updatedAt: new Date() })
    .where(eq(strategyMandates.mandateId, binding.mandateId));

  return inserted;
}

export async function readVault(
  chainId: number,
  vaultAddress: string,
  db: Db = defaultDb,
): Promise<VaultRow | null> {
  const [vault] = await db
    .select()
    .from(vaults)
    .where(
      and(eq(vaults.chainId, chainId), eq(vaults.vaultAddress, vaultAddress)),
    );

  return vault ?? null;
}

// The vault a given mandate executes through (at most one — `mandate_id` is
// unique). Returns null until the mandate is bound to a deployed vault.
export async function readVaultForMandate(
  mandateId: string,
  db: Db = defaultDb,
): Promise<VaultRow | null> {
  const [vault] = await db
    .select()
    .from(vaults)
    .where(eq(vaults.mandateId, mandateId));

  return vault ?? null;
}
