from __future__ import annotations

from datetime import date, datetime, time, timezone

import pandas as pd

from scripts.ingestion import refresh_history


def _unix_ms(day: date, *, hour: int = 0) -> int:
    return int(
        datetime.combine(day, time(hour=hour), tzinfo=timezone.utc).timestamp() * 1000
    )


class _FakeStorage:
    def __init__(self, datasets: dict[str, pd.DataFrame | None]) -> None:
        self.datasets = datasets
        self.bucket = "test-bucket"
        self.prefix = "dev"
        self.writes: dict[str, pd.DataFrame] = {}

    def read_dataset(self, dataset: str) -> pd.DataFrame | None:
        frame = self.datasets.get(dataset)
        if frame is None:
            return None
        return frame.copy()

    def write_dataset(self, dataset: str, frame: pd.DataFrame) -> str:
        self.writes[dataset] = frame.copy()
        return f"dev/datasets/{dataset}.parquet"

    def dataset_uri(self, dataset: str) -> str:
        return f"s3://test-bucket/dev/datasets/{dataset}.parquet"


class _FakeCoinGeckoClient:
    def __init__(self) -> None:
        self.market_calls: list[dict[str, object]] = []
        self.history_calls: list[dict[str, object]] = []

    def get_markets(
        self, *, vs_currency: str, order: str, per_page: int, page: int, sparkline: bool
    ) -> list[dict[str, object]]:
        self.market_calls.append({"page": page, "per_page": per_page})
        if page > 1:
            return []
        return [
            {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "image": "btc.png",
                "market_cap_rank": 1,
                "market_cap": 300.0,
            },
            {
                "id": "ethereum",
                "symbol": "eth",
                "name": "Ethereum",
                "image": "eth.png",
                "market_cap_rank": 2,
                "market_cap": 200.0,
            },
        ]

    def get_market_chart_range(
        self,
        coin_id: str,
        *,
        vs_currency: str,
        from_unix: int,
        to_unix: int,
        interval: str,
    ) -> dict[str, list[list[float]]]:
        self.history_calls.append(
            {
                "coin_id": coin_id,
                "from_unix": from_unix,
                "to_unix": to_unix,
                "interval": interval,
                "vs_currency": vs_currency,
            }
        )
        base = 100.0 if coin_id == "bitcoin" else 10.0
        return {
            "prices": [
                [_unix_ms(date(2024, 1, 3), hour=1), base],
                [_unix_ms(date(2024, 1, 3), hour=23), base + 1],
                [_unix_ms(date(2024, 1, 4), hour=1), base + 2],
            ],
            "market_caps": [
                [_unix_ms(date(2024, 1, 3)), base * 10],
                [_unix_ms(date(2024, 1, 4)), base * 10 + 2],
            ],
            "total_volumes": [
                [_unix_ms(date(2024, 1, 3)), base * 5],
                [_unix_ms(date(2024, 1, 4)), base * 5 + 2],
            ],
        }


def test_series_frame_keeps_last_sample_per_day() -> None:
    frame = refresh_history._series_frame(
        "bitcoin",
        [
            [_unix_ms(date(2024, 1, 3), hour=1), 100.0],
            [_unix_ms(date(2024, 1, 3), hour=20), 101.0],
        ],
        value_column="price",
    )

    assert frame.to_dict("records") == [
        {"date": date(2024, 1, 3), "coin_id": "bitcoin", "price": 101.0}
    ]


def test_run_upserts_history_and_metadata() -> None:
    storage = _FakeStorage(
        {
            "daily_prices": pd.DataFrame(
                [{"date": "2024-01-03", "coin_id": "bitcoin", "price": 99.0}]
            ),
            "daily_market_caps": pd.DataFrame(
                [
                    {
                        "date": "2024-01-02",
                        "coin_id": "bitcoin",
                        "market_cap": 990.0,
                    }
                ]
            ),
            "daily_volumes": pd.DataFrame(
                [{"date": "2024-01-03", "coin_id": "bitcoin", "volume": 490.0}]
            ),
            "coin_metadata": pd.DataFrame(
                [
                    {
                        "coin_id": "solana",
                        "symbol": "sol",
                        "name": "Solana",
                        "market_cap_rank": 3,
                    }
                ]
            ),
        }
    )
    client = _FakeCoinGeckoClient()

    payload = refresh_history.run(
        storage=storage,
        client=client,
        snapshot_date=date(2024, 1, 4),
        vs_currency="usd",
        bootstrap_days=7,
    )

    assert payload["coins"] == 2
    assert storage.writes["daily_prices"].to_dict("records") == [
        {"date": date(2024, 1, 3), "coin_id": "bitcoin", "price": 101.0},
        {"date": date(2024, 1, 3), "coin_id": "ethereum", "price": 11.0},
        {"date": date(2024, 1, 4), "coin_id": "bitcoin", "price": 102.0},
        {"date": date(2024, 1, 4), "coin_id": "ethereum", "price": 12.0},
    ]
    assert storage.writes["daily_market_caps"].to_dict("records") == [
        {"date": date(2024, 1, 2), "coin_id": "bitcoin", "market_cap": 990.0},
        {"date": date(2024, 1, 3), "coin_id": "bitcoin", "market_cap": 1000.0},
        {"date": date(2024, 1, 3), "coin_id": "ethereum", "market_cap": 100.0},
        {"date": date(2024, 1, 4), "coin_id": "bitcoin", "market_cap": 1002.0},
        {"date": date(2024, 1, 4), "coin_id": "ethereum", "market_cap": 102.0},
    ]
    assert storage.writes["daily_volumes"].to_dict("records") == [
        {"date": date(2024, 1, 3), "coin_id": "bitcoin", "volume": 500.0},
        {"date": date(2024, 1, 3), "coin_id": "ethereum", "volume": 50.0},
        {"date": date(2024, 1, 4), "coin_id": "bitcoin", "volume": 502.0},
        {"date": date(2024, 1, 4), "coin_id": "ethereum", "volume": 52.0},
    ]
    assert storage.writes["coin_metadata"]["coin_id"].tolist() == [
        "bitcoin",
        "ethereum",
        "solana",
    ]
    assert client.history_calls[0]["coin_id"] == "bitcoin"
    assert client.history_calls[0]["from_unix"] == int(
        datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()
    )
    assert client.history_calls[1]["coin_id"] == "ethereum"
    assert client.history_calls[1]["from_unix"] == int(
        datetime(2023, 12, 28, tzinfo=timezone.utc).timestamp()
    )
