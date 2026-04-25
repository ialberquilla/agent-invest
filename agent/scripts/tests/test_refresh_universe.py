from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from scripts.ingestion import refresh_universe


class _FakeCoinGeckoClient:
    def __init__(self, pages: list[list[dict[str, object]]]) -> None:
        self.pages = pages
        self.calls: list[dict[str, object]] = []

    def get_markets(
        self, *, vs_currency: str, order: str, per_page: int, page: int, sparkline: bool
    ) -> list[dict[str, object]]:
        self.calls.append(
            {
                "vs_currency": vs_currency,
                "order": order,
                "per_page": per_page,
                "page": page,
                "sparkline": sparkline,
            }
        )

        if page > len(self.pages):
            return []

        return self.pages[page - 1]


def test_fetch_universe_snapshot_paginates_and_normalizes() -> None:
    client = _FakeCoinGeckoClient(
        [
            [
                {
                    "id": "ethereum",
                    "symbol": "eth",
                    "name": "Ethereum",
                    "market_cap_rank": 2,
                    "market_cap": 200.0,
                    "last_updated": "2024-01-02T00:00:00Z",
                },
                {
                    "id": "bitcoin",
                    "symbol": "btc",
                    "name": "Bitcoin",
                    "market_cap_rank": 1,
                    "market_cap": 300.0,
                    "last_updated": "2024-01-02T00:00:00Z",
                },
            ],
            [],
        ]
    )

    snapshot = refresh_universe.fetch_universe_snapshot(
        client,
        snapshot_date=date(2024, 1, 2),
        vs_currency="usd",
        page_size=2,
    )

    assert list(snapshot["coin_id"]) == ["bitcoin", "ethereum"]
    assert snapshot["date"].dt.date.tolist() == [date(2024, 1, 2), date(2024, 1, 2)]
    assert [call["page"] for call in client.calls] == [1, 2]


def test_merge_universe_history_replaces_same_day_snapshot() -> None:
    existing = pd.DataFrame(
        [
            {"date": "2024-01-01", "coin_id": "bitcoin", "market_cap": 100.0},
            {"date": "2024-01-02", "coin_id": "ethereum", "market_cap": 150.0},
        ]
    )
    snapshot = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2024-01-02"),
                "coin_id": "bitcoin",
                "market_cap_rank": 1,
                "market_cap": 300.0,
            }
        ]
    )

    merged = refresh_universe.merge_universe_history(existing, snapshot)

    assert merged["coin_id"].tolist() == ["bitcoin", "bitcoin"]
    assert pd.to_datetime(merged["date"]).dt.date.tolist() == [
        date(2024, 1, 1),
        date(2024, 1, 2),
    ]
    assert merged["market_cap"].tolist() == [100.0, 300.0]
    assert pd.isna(merged.iloc[0]["market_cap_rank"])
    assert merged.iloc[1]["market_cap_rank"] == 1


def test_fetch_universe_snapshot_rejects_empty_payload() -> None:
    client = _FakeCoinGeckoClient([[]])

    with pytest.raises(RuntimeError, match="empty universe snapshot"):
        refresh_universe.fetch_universe_snapshot(
            client,
            snapshot_date=date(2024, 1, 2),
            vs_currency="usd",
            page_size=100,
        )
