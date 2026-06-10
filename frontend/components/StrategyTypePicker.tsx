"use client";

import { useState } from "react";

import { AllocationWizard } from "@/components/AllocationWizard";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import type { AllocationWizardState } from "@/lib/wizard-prompt";
import {
  buildStrategyOverrides,
  DEFAULT_NON_BASKET_FIELDS,
  STRATEGY_TYPE_OPTIONS,
  validateNonBasket,
  type NonBasketFields,
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
          {type === "single_asset" ? (
            <Field
              label="Coin id"
              hint="The single market to trade, e.g. bitcoin."
              value={fields.targetCoin}
              onChange={(v) => update({ targetCoin: v })}
              placeholder="bitcoin"
              disabled={disabled}
            />
          ) : null}

          {type === "pair_trade" ? (
            <>
              <Field
                label="Long coin id"
                hint="The market to go long."
                value={fields.longCoin}
                onChange={(v) => update({ longCoin: v })}
                placeholder="ethereum"
                disabled={disabled}
              />
              <Field
                label="Short coin id"
                hint="The market to short against it."
                value={fields.shortCoin}
                onChange={(v) => update({ shortCoin: v })}
                placeholder="bitcoin"
                disabled={disabled}
              />
            </>
          ) : null}

          {type === "long_short" ? (
            <Field
              label="Ranking pool size"
              hint="How many assets to rank before going long the strongest and short the weakest (min 4)."
              value={String(fields.poolSize)}
              onChange={(v) => update({ poolSize: Number(v) || 0 })}
              placeholder="8"
              type="number"
              disabled={disabled}
            />
          ) : null}

          {error ? (
            <p className="text-sm text-destructive">{error}</p>
          ) : null}

          <Button type="button" onClick={runNonBasket} disabled={disabled}>
            Run backtest
          </Button>
        </div>
      )}
    </div>
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
