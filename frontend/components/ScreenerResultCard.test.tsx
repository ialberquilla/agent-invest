import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ScreenerResultCard } from "@/components/ScreenerResultCard";
import type { ScreenerResult } from "@/lib/types";

vi.mock("@privy-io/react-auth", () => ({
  usePrivy: () => ({ authenticated: true, login: vi.fn() }),
  useWallets: () => ({ wallets: [] }),
}));

const result: ScreenerResult = {
  type: "market_screener",
  version: 1,
  title: "GMX momentum screener",
  summary: "Only rows with executable Arbitrum GMX V2 markets are shown.",
  definition: { factor: "momentum", limit: 2, gmx_only: false },
  notes: ["Actions open a confirmation ticket."],
  rows: [
    {
      rank: 1,
      source_rank: 1,
      coin_id: "bitcoin",
      symbol: "BTC",
      market_name: "BTC/USD [BTC-USDC]",
      is_gmx_tradeable: true,
      factor_values: { return_180d: 0.42, sharpe_180d: 1.2, volatility_180d: 0.7 },
      metrics: [
        { id: "return_180d", label: "180d return", value: 0.42, format: "percent" },
        { id: "sharpe_180d", label: "180d Sharpe", value: 1.2, format: "number" },
      ],
      actions: {
        long: { enabled: true, label: "Long BTC" },
        short: { enabled: true, label: "Short BTC" },
      },
      gmx_market: {
        chain: "arbitrum",
        market_token: "0xmarket",
        index_token: "0xindex",
        long_token: "0xlong",
        short_token: "0xshort",
        collateral_token: "0xusdc",
        collateral_decimals: 6,
      },
    },
    {
      rank: 2,
      source_rank: 2,
      coin_id: "research-coin",
      symbol: "RESEARCH-COIN",
      market_name: null,
      is_gmx_tradeable: false,
      factor_values: { return_180d: 0.3, sharpe_180d: 0.8 },
      metrics: [
        { id: "return_180d", label: "180d return", value: 0.3, format: "percent" },
      ],
      actions: {
        long: { enabled: false, label: "Research only" },
        short: { enabled: false, label: "Research only" },
      },
      gmx_market: null,
    },
  ],
};

describe("ScreenerResultCard", () => {
  it("renders ranked metrics and gates non-GMX actions", () => {
    render(<ScreenerResultCard result={result} />);

    expect(screen.getByText("GMX momentum screener")).toBeInTheDocument();
    expect(screen.getByText("BTC")).toBeInTheDocument();
    expect(screen.getByText("42.0%")).toBeInTheDocument();
    expect(screen.getByText("Research only")).toBeInTheDocument();

    const rows = screen.getAllByRole("row");
    const btcRow = rows.find((row) => within(row).queryByText("BTC"));
    const researchRow = rows.find((row) =>
      within(row).queryByText("RESEARCH-COIN"),
    );
    expect(within(btcRow!).getByRole("button", { name: /Long/i })).toBeEnabled();
    expect(
      within(researchRow!).getByRole("button", { name: /Long/i }),
    ).toBeDisabled();
  });

  it("opens an in-app GMX order ticket", () => {
    render(<ScreenerResultCard result={result} />);

    fireEvent.click(screen.getAllByRole("button", { name: /Short/i })[0]);

    expect(screen.getByText("Short BTC on GMX")).toBeInTheDocument();
    expect(screen.getByText("Creates a GMX V2 market increase order", { exact: false })).toBeInTheDocument();
    expect(screen.getByLabelText(/Collateral amount/i)).toHaveValue("100");
    expect(screen.getByLabelText(/Slippage tolerance/i)).toHaveValue("1");
    expect(screen.getByRole("button", { name: /Trade Short on GMX/i })).toBeEnabled();
  });
});
