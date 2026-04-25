"""S3-backed parquet dataset helpers for ingestion refresh jobs."""

from __future__ import annotations

import io
import os
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Literal, TypeAlias

import boto3
import pandas as pd
from botocore.exceptions import ClientError

DatasetName: TypeAlias = Literal[
    "universe_history",
    "daily_prices",
    "daily_market_caps",
    "daily_volumes",
    "coin_metadata",
]

_DATASET_FILES: dict[DatasetName, str] = {
    "universe_history": "universe_history.parquet",
    "daily_prices": "daily_prices.parquet",
    "daily_market_caps": "daily_market_caps.parquet",
    "daily_volumes": "daily_volumes.parquet",
    "coin_metadata": "coin_metadata.parquet",
}
_PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"
_REPO_ROOT = Path(__file__).resolve().parents[4]
_DEFAULT_CACHE_DIR = _REPO_ROOT / ".data" / "cache" / "datasets"


def _read_required_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "").strip()

        if value:
            return value

    formatted_names = ", ".join(names)
    raise RuntimeError(f"Missing required environment variable(s): {formatted_names}")


def _read_optional_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name, "").strip()

        if value:
            return value

    return None


def _normalize_prefix(prefix: str | None) -> str:
    if not prefix:
        return ""

    return prefix.strip("/")


def _join_key(*segments: str) -> str:
    return "/".join(segment for segment in segments if segment)


def _write_cache_file(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None

    try:
        with NamedTemporaryFile(dir=path.parent, delete=False) as handle:
            handle.write(payload)
            temp_path = Path(handle.name)

        temp_path.replace(path)
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


def _is_missing_dataset_error(error: ClientError) -> bool:
    error_code = error.response.get("Error", {}).get("Code")
    return error_code in {"404", "NoSuchKey", "NotFound"}


@dataclass(slots=True)
class DatasetStorage:
    bucket: str = field(
        default_factory=lambda: _read_required_env("S3_BUCKET", "AWS_S3_BUCKET")
    )
    region: str | None = field(
        default_factory=lambda: _read_optional_env("AWS_REGION", "AWS_DEFAULT_REGION")
    )
    prefix: str = field(
        default_factory=lambda: _normalize_prefix(
            _read_optional_env("S3_PREFIX", "AWS_S3_PREFIX")
        )
    )
    cache_dir: Path = field(default_factory=lambda: _DEFAULT_CACHE_DIR)
    s3_client: Any = field(default=None)

    def __post_init__(self) -> None:
        if self.s3_client is None:
            if self.region:
                self.s3_client = boto3.client("s3", region_name=self.region)
            else:
                self.s3_client = boto3.client("s3")

    def dataset_key(self, dataset: DatasetName) -> str:
        return _join_key(self.prefix, "datasets", _DATASET_FILES[dataset])

    def dataset_uri(self, dataset: DatasetName) -> str:
        return f"s3://{self.bucket}/{self.dataset_key(dataset)}"

    def cache_path(self, dataset: DatasetName) -> Path:
        return self.cache_dir / _DATASET_FILES[dataset]

    def read_dataset(self, dataset: DatasetName) -> pd.DataFrame | None:
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=self.dataset_key(dataset),
            )
        except ClientError as error:
            if _is_missing_dataset_error(error):
                return None

            raise

        payload = response["Body"].read()
        self._write_cache(dataset, payload)
        return pd.read_parquet(io.BytesIO(payload))

    def write_dataset(self, dataset: DatasetName, frame: pd.DataFrame) -> str:
        payload = self._serialize_parquet(frame)
        key = self.dataset_key(dataset)

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=payload,
            ContentType=_PARQUET_CONTENT_TYPE,
        )
        self._write_cache(dataset, payload)
        return key

    def _serialize_parquet(self, frame: pd.DataFrame) -> bytes:
        buffer = io.BytesIO()
        frame.to_parquet(buffer, index=False)
        return buffer.getvalue()

    def _write_cache(self, dataset: DatasetName, payload: bytes) -> None:
        _write_cache_file(self.cache_path(dataset), payload)
