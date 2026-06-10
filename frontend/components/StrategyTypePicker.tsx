"use client";

import { useEffect, useState } from "react";

import { AllocationWizard } from "@/components/AllocationWizard";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import type { AllocationWizardState } from "@/lib/wizard-prompt";
import {
  buildStrategyOverrides,
  DEFAULT_NON_BASKET_FIELDS,
  HORIZON_OPTIONS,
  rotationPerSide,
  SIGNAL_SPEED_OPTIONS,
  STRATEGY_TYPE_OPTIONS,
  validateNonBasket,
  type HorizonPreset,
  type NonBasketFields,
  type SignalSpeed,
  type WizardStrategyType,
} from "@/lib/strategy-types";

type Props = {
  // Basket runs the full allocation wizard (unchanged).
  onSubmitBasket: (state: AllocationWizardState) => void | Promise<void>;
  // Non-basket types submit a deterministic overrides object + a label.
  onSubmitOverrides: (
    overrides: Record<string, unknown>,
    label: string,
  ) => void | Promise<void>;
  disabled?: boolean;
};

type GmxCoin = { coin_id: string; symbol: string };

export function StrategyTypePicker({
  onSubmitBasket,
  onSubmitOverrides,
  disabled = false,
}: Props) {
  const [type, setType] = useState<WizardStrategyType>("basket");
  const [fields, setFields] = useState<NonBasketFields>(
    DEFAULT_NON_BASKET_FIELDS,
  );
  const [error, setError] = useState<string | null>(null);
  const [coins, setCoins] = useState<GmxCoin[] | null>(null);
  const [coinsError, setCoinsError] = useState<string | null>(null);

  // Load the GMX-tradeable coin set once a non-basket type is in play, so the
  // user picks from a list instead of typing a coin_id.
  const needsCoins = type === "single_asset" || type === "pair_trade";
  useEffect(() => {
    if (!needsCoins || coins !== null) return;
    let cancelled = false;
    void (async () => {
      try {
        const response = await fetch("/api/markets/gmx", { cache: "no-store" });
        if (!response.ok) throw new Error("Unable to load tradeable markets");
        const payload = (await response.json()) as { coins?: GmxCoin[] };
        if (!cancelled) setCoins(payload.coins ?? []);
      } catch (err) {
        if (!cancelled) {
          setCoinsError(
            err instanceof Error ? err.message : "Unable to load markets",
          );
          setCoins([]);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [needsCoins, coins]);

  function update(patch: Partial<NonBasketFields>) {
    setFields((current) => ({ ...current, ...patch }));
    setError(null);
  }

  async function runNonBasket() {
    const validation = validateNonBasket(type, fields);
    if (!validation.ok) {
      setError(validation.message);
      return;
    }
    const overrides = buildStrategyOverrides(type, fields);
    if (!overrides) return;
    const label =
      STRATEGY_TYPE_OPTIONS.find((o) => o.value === type)?.label ?? "Strategy";
    await onSubmitOverrides(overrides, label);
  }

  return (
    <div className="space-y-4">
      <div>
        <h3 className="font-heading text-sm font-semibold tracking-tight">
          Strategy type
        </h3>
        <p className="text-sm text-subtle-foreground">
          Pick the shape of the strategy. A diversified basket runs the full
          guided setup; the others need only a couple of inputs.
        </p>
      </div>

      <div className="grid gap-2 sm:grid-cols-2">
        {STRATEGY_TYPE_OPTIONS.map((option) => {
          const active = option.value === type;
          return (
            <button
              key={option.value}
              type="button"
              disabled={disabled}
              onClick={() => {
                setType(option.value);
                setError(null);
              }}
              className={`rounded-xl border p-3 text-left shadow-xs transition ${
                active
                  ? "border-primary bg-primary/5 ring-1 ring-primary"
                  : "border-border bg-card hover:bg-muted/40"
              }`}
            >
              <div className="text-sm font-semibold">{option.label}</div>
              <div className="mt-1 text-xs text-muted-foreground">
                {option.description}
              </div>
            </button>
          );
        })}
      </div>

      {type === "basket" ? (
        <AllocationWizard embedded onSubmit={onSubmitBasket} />
      ) : (
        <div className="space-y-3 rounded-xl border bg-card p-4 shadow-xs">
          {coinsError ? (
            <p className="text-sm text-destructive">{coinsError}</p>
          ) : null}

          {type === "single_asset" ? (
            <>
              <CoinSelect
                label="Market"
                hint="The single market to trade, long when trending up."
                value={fields.targetCoin}
                onChange={(v) => update({ targetCoin: v })}
                coins={coins}
                disabled={disabled}
              />
              <SingleAssetAssumptions
                fields={fields}
                update={update}
                disabled={disabled}
              />
            </>
          ) : null}

          {type === "pair_trade" ? (
            <>
              <CoinSelect
                label="Long market"
                hint="The market to go long."
                value={fields.longCoin}
                onChange={(v) => update({ longCoin: v })}
                coins={coins}
                disabled={disabled}
              />
              <CoinSelect
                label="Short market"
                hint="The market to short against it."
                value={fields.shortCoin}
                onChange={(v) => update({ shortCoin: v })}
                coins={coins}
                disabled={disabled}
              />
            </>
          ) : null}

          {type === "long_short" ? (
            <div className="space-y-1">
              <Field
                label="Ranking pool size"
                hint="How many assets to rank before going long the strongest and short the weakest (min 4)."
                value={String(fields.poolSize)}
                onChange={(v) => update({ poolSize: Number(v) || 0 })}
                placeholder="8"
                type="number"
                disabled={disabled}
              />
              {fields.poolSize >= 4 ? (
                <p className="text-xs text-subtle-foreground">
                  Holds the {rotationPerSide(fields.poolSize)} strongest long
                  and {rotationPerSide(fields.poolSize)} weakest short —{" "}
                  {rotationPerSide(fields.poolSize) * 2} positions from a{" "}
                  {Math.max(4, Math.round(fields.poolSize))}-asset pool.
                </p>
              ) : null}
            </div>
          ) : null}

          {error ? (
            <p className="text-sm text-destructive">{error}</p>
          ) : null}

          <Button
            type="button"
            onClick={runNonBasket}
            disabled={disabled || (needsCoins && coins === null)}
          >
            {needsCoins && coins === null ? "Loading markets…" : "Run backtest"}
          </Button>
        </div>
      )}
    </div>
  );
}

// Compact, mostly-read-only summary of the assumptions a single-asset run
// uses, with the few high-value choices (horizon, max drawdown, signal speed)
// editable. Everything else comes from the neutral base brief in ChatView and
// is shown here so the guided setup isn't a one-field flow with hidden config.
function SingleAssetAssumptions({
  fields,
  update,
  disabled,
}: {
  fields: NonBasketFields;
  update: (patch: Partial<NonBasketFields>) => void;
  disabled?: boolean;
}) {
  const lookbacks =
    SIGNAL_SPEED_OPTIONS.find((o) => o.value === fields.signalSpeed)
      ?.lookbacks ?? [];
  return (
    <div className="space-y-3 rounded-lg border border-dashed bg-muted/30 p-3">
      <div>
        <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          Assumptions
        </h4>
        <p className="mt-0.5 text-xs text-subtle-foreground">
          These are used to backtest the setup. Adjust the few below; the rest
          are fixed for the guided flow.
        </p>
      </div>

      <div className="grid gap-3 sm:grid-cols-3">
        <SelectField
          label="Horizon"
          hint="Backtest lookback window."
          value={fields.horizon}
          onChange={(v) => update({ horizon: v as HorizonPreset })}
          disabled={disabled}
          options={HORIZON_OPTIONS.map((o) => ({
            value: o.value,
            label: o.label,
          }))}
        />
        <SelectField
          label="Max drawdown"
          hint="Largest peak-to-trough loss tolerated."
          value={String(fields.maxDrawdownPct)}
          onChange={(v) => update({ maxDrawdownPct: Number(v) })}
          disabled={disabled}
          options={[
            { value: "20", label: "20%" },
            { value: "35", label: "35%" },
            { value: "50", label: "50%" },
          ]}
        />
        <SelectField
          label="Signal speed"
          hint={
            lookbacks.length
              ? `SMA sweep: ${lookbacks.join(" / ")} days.`
              : "Trend-signal responsiveness."
          }
          value={fields.signalSpeed}
          onChange={(v) => update({ signalSpeed: v as SignalSpeed })}
          disabled={disabled}
          options={SIGNAL_SPEED_OPTIONS.map((o) => ({
            value: o.value,
            label: o.label,
          }))}
        />
      </div>

      <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-subtle-foreground">
        <ReadOnlyAssumption label="Universe" value="Top 25 by market cap" />
        <ReadOnlyAssumption label="Min market cap" value="$1B+" />
        <ReadOnlyAssumption label="Direction" value="Long / flat (no shorts)" />
        <ReadOnlyAssumption label="Rebalance" value="Monthly" />
        <ReadOnlyAssumption label="Cash buffer" value="None" />
        <ReadOnlyAssumption label="Risk profile" value="Balanced" />
      </dl>
    </div>
  );
}

function ReadOnlyAssumption({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between gap-2">
      <dt>{label}</dt>
      <dd className="font-medium text-foreground">{value}</dd>
    </div>
  );
}

function SelectField({
  label,
  hint,
  value,
  onChange,
  options,
  disabled,
}: {
  label: string;
  hint: string;
  value: string;
  onChange: (value: string) => void;
  options: { value: string; label: string }[];
  disabled?: boolean;
}) {
  return (
    <label className="block space-y-1">
      <span className="text-sm font-medium">{label}</span>
      <select
        value={value}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm text-foreground shadow-xs outline-none [color-scheme:light] focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50 dark:[color-scheme:dark]"
      >
        {options.map((option) => (
          <option
            key={option.value}
            value={option.value}
            className="bg-background text-foreground"
          >
            {option.label}
          </option>
        ))}
      </select>
      <span className="block text-xs text-muted-foreground">{hint}</span>
    </label>
  );
}

// "bitcoin" -> "Bitcoin", "avalanche-2" -> "Avalanche 2".
function coinName(coinId: string): string {
  return coinId
    .split(/[-_]/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function CoinSelect({
  label,
  hint,
  value,
  onChange,
  coins,
  disabled,
}: {
  label: string;
  hint: string;
  value: string;
  onChange: (value: string) => void;
  coins: GmxCoin[] | null;
  disabled?: boolean;
}) {
  return (
    <label className="block space-y-1">
      <span className="text-sm font-medium">{label}</span>
      <select
        value={value}
        disabled={disabled || coins === null}
        onChange={(event) => onChange(event.target.value)}
        // Native selects ignore bg-transparent for the control and the popup
        // options, so set themed colors explicitly (and color-scheme so the
        // browser paints the dropdown in dark mode).
        className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm text-foreground shadow-xs outline-none [color-scheme:light] focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50 dark:[color-scheme:dark]"
      >
        <option value="" className="bg-background text-foreground">
          {coins === null ? "Loading markets…" : "Select a market"}
        </option>
        {(coins ?? []).map((coin) => (
          <option
            key={coin.coin_id}
            value={coin.coin_id}
            className="bg-background text-foreground"
          >
            {coin.symbol} · {coinName(coin.coin_id)}
          </option>
        ))}
      </select>
      <span className="block text-xs text-muted-foreground">{hint}</span>
    </label>
  );
}

function Field({
  label,
  hint,
  value,
  onChange,
  placeholder,
  type = "text",
  disabled,
}: {
  label: string;
  hint: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  type?: "text" | "number";
  disabled?: boolean;
}) {
  return (
    <label className="block space-y-1">
      <span className="text-sm font-medium">{label}</span>
      <Input
        type={type}
        value={value}
        placeholder={placeholder}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
      />
      <span className="block text-xs text-muted-foreground">{hint}</span>
    </label>
  );
}
