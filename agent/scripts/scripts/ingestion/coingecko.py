"""CoinGecko HTTP client for ingestion-only refresh jobs.

This module is intentionally kept out of the agent-facing import surface. Only
scheduled ingestion jobs should use it.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any

import httpx


@dataclass(slots=True)
class CoinGeckoClient:
    base_url: str = field(
        default_factory=lambda: os.getenv(
            "COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"
        )
    )
    api_key: str | None = field(default_factory=lambda: os.getenv("COINGECKO_API_KEY"))
    api_key_header: str = field(
        default_factory=lambda: os.getenv(
            "COINGECKO_API_KEY_HEADER", "x-cg-demo-api-key"
        )
    )
    min_interval_seconds: float = field(
        default_factory=lambda: float(
            os.getenv("COINGECKO_MIN_INTERVAL_SECONDS", "2.2")
        )
    )
    timeout_seconds: float = field(
        default_factory=lambda: float(os.getenv("COINGECKO_TIMEOUT_SECONDS", "30"))
    )
    max_retries: int = field(
        default_factory=lambda: int(os.getenv("COINGECKO_MAX_RETRIES", "5"))
    )
    _client: httpx.Client = field(init=False, repr=False)
    _last_request_monotonic: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        headers = {
            "accept": "application/json",
            "user-agent": "agent-invest/0.1",
        }
        if self.api_key:
            headers[self.api_key_header] = self.api_key

        self._client = httpx.Client(
            base_url=self.base_url, headers=headers, timeout=self.timeout_seconds
        )

    def __enter__(self) -> "CoinGeckoClient":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def ping(self) -> dict[str, Any]:
        return self._request_json("GET", "/ping", {})

    def get_markets(
        self, *, vs_currency: str, order: str, per_page: int, page: int, sparkline: bool
    ) -> list[dict[str, Any]]:
        return self._request_json(
            "GET",
            "/coins/markets",
            {
                "vs_currency": vs_currency,
                "order": order,
                "per_page": per_page,
                "page": page,
                "sparkline": str(sparkline).lower(),
            },
        )

    def get_market_chart_range(
        self,
        coin_id: str,
        *,
        vs_currency: str,
        from_unix: int,
        to_unix: int,
        interval: str = "daily",
    ) -> dict[str, Any]:
        return self._request_json(
            "GET",
            f"/coins/{coin_id}/market_chart/range",
            {
                "vs_currency": vs_currency,
                "from": from_unix,
                "to": to_unix,
                "interval": interval,
            },
        )

    def _request_json(self, method: str, url: str, params: dict[str, Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            self._respect_rate_limit()
            try:
                response = self._client.request(method, url, params=params)
                self._last_request_monotonic = time.monotonic()
            except httpx.HTTPError as error:
                last_error = error
                if attempt == self.max_retries:
                    raise RuntimeError(
                        f"CoinGecko request failed for {url}: {error}"
                    ) from error
                time.sleep(self._retry_delay(attempt, None))
                continue

            if response.status_code == 429 or response.status_code >= 500:
                if attempt == self.max_retries:
                    response.raise_for_status()
                time.sleep(self._retry_delay(attempt, response))
                continue

            if response.status_code == 401:
                if self.api_key:
                    raise RuntimeError(
                        "CoinGecko returned 401 Unauthorized. Check "
                        "COINGECKO_API_KEY and COINGECKO_API_KEY_HEADER."
                    )
                raise RuntimeError(
                    "CoinGecko returned 401 Unauthorized. Set "
                    "COINGECKO_API_KEY in the environment before fetching "
                    "CoinGecko data."
                )

            response.raise_for_status()
            return response.json()

        raise RuntimeError(
            f"CoinGecko request exhausted retries for {url}: {last_error}"
        )

    def _respect_rate_limit(self) -> None:
        if not self._last_request_monotonic:
            return

        elapsed = time.monotonic() - self._last_request_monotonic
        remaining = self.min_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _retry_delay(self, attempt: int, response: httpx.Response | None) -> float:
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    return max(float(retry_after), self.min_interval_seconds)
                except ValueError:
                    pass

        return min(60.0, self.min_interval_seconds * (2 ** (attempt - 1)))
