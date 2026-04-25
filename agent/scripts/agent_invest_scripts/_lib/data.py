"""Dataset readers backed by S3 with a local parquet cache."""

from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Final, Literal, TypeAlias

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

_DATASET_FILES: Final[dict[DatasetName, str]] = {
    "universe_history": "universe_history.parquet",
    "daily_prices": "daily_prices.parquet",
    "daily_market_caps": "daily_market_caps.parquet",
    "daily_volumes": "daily_volumes.parquet",
    "coin_metadata": "coin_metadata.parquet",
}
_REPO_ROOT = Path(__file__).resolve().parents[4]
_DEFAULT_CACHE_DIR = _REPO_ROOT / ".data" / "cache" / "datasets"


class DatasetNotFoundError(FileNotFoundError):
    """Raised when a requested dataset is missing from S3 and the cache."""

    def __init__(self, dataset: DatasetName, *, bucket: str, key: str) -> None:
        super().__init__(f'Dataset "{dataset}" not found at s3://{bucket}/{key}')
        self.dataset = dataset
        self.bucket = bucket
        self.key = key


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


def _cache_dir() -> Path:
    configured_path = _read_optional_env("DATASET_CACHE_DIR")

    if not configured_path:
        return _DEFAULT_CACHE_DIR

    path = Path(configured_path).expanduser()

    if path.is_absolute():
        return path

    return (_REPO_ROOT / path).resolve()


def _cache_path(dataset: DatasetName) -> Path:
    return _cache_dir() / _DATASET_FILES[dataset]


def _dataset_key(dataset: DatasetName) -> str:
    prefix = _normalize_prefix(_read_optional_env("S3_PREFIX", "AWS_S3_PREFIX"))
    return _join_key(prefix, "datasets", _DATASET_FILES[dataset])


def _bucket_name() -> str:
    return _read_required_env("S3_BUCKET", "AWS_S3_BUCKET")


def _s3_client() -> Any:
    region = _read_optional_env("AWS_REGION", "AWS_DEFAULT_REGION")

    if region:
        return boto3.client("s3", region_name=region)

    return boto3.client("s3")


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


def _read_body_bytes(body: Any) -> bytes:
    if hasattr(body, "read"):
        payload = body.read()
    else:
        payload = body

    if isinstance(payload, bytes):
        return payload

    raise TypeError("S3 response body must be bytes")


def _is_missing_dataset_error(error: ClientError) -> bool:
    error_code = error.response.get("Error", {}).get("Code")
    return error_code in {"404", "NoSuchKey", "NotFound"}


def read_dataset(dataset: DatasetName) -> pd.DataFrame:
    """Load a parquet dataset from the local cache, fetching from S3 on miss."""
    cache_path = _cache_path(dataset)

    if cache_path.exists():
        return pd.read_parquet(cache_path)

    bucket = _bucket_name()
    key = _dataset_key(dataset)

    try:
        response = _s3_client().get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if _is_missing_dataset_error(error):
            raise DatasetNotFoundError(dataset, bucket=bucket, key=key) from error

        raise

    _write_cache_file(cache_path, _read_body_bytes(response["Body"]))
    return pd.read_parquet(cache_path)


def universe_history() -> pd.DataFrame:
    """Return the cached `universe_history` dataset."""
    return read_dataset("universe_history")


def daily_prices() -> pd.DataFrame:
    """Return the cached `daily_prices` dataset."""
    return read_dataset("daily_prices")


def daily_market_caps() -> pd.DataFrame:
    """Return the cached `daily_market_caps` dataset."""
    return read_dataset("daily_market_caps")


def daily_volumes() -> pd.DataFrame:
    """Return the cached `daily_volumes` dataset."""
    return read_dataset("daily_volumes")


def coin_metadata() -> pd.DataFrame:
    """Return the cached `coin_metadata` dataset."""
    return read_dataset("coin_metadata")


__all__ = [
    "DatasetName",
    "DatasetNotFoundError",
    "coin_metadata",
    "daily_market_caps",
    "daily_prices",
    "daily_volumes",
    "read_dataset",
    "universe_history",
]
