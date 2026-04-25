from __future__ import annotations

import io
import tempfile
import unittest
from unittest import mock

import pandas as pd
from botocore.exceptions import ClientError

from agent_invest_scripts._lib import data


def _fixture_parquet_bytes() -> bytes:
    frame = pd.DataFrame(
        [
            {"date": "2024-01-01", "coin_id": "bitcoin", "market_cap": 1.0},
            {"date": "2024-01-02", "coin_id": "ethereum", "market_cap": 2.0},
        ]
    )
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


class _FakeS3Client:
    def __init__(self, payload: bytes, *, missing: bool = False) -> None:
        self.payload = payload
        self.missing = missing
        self.calls: list[tuple[str, str]] = []

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, io.BytesIO]:
        self.calls.append((Bucket, Key))

        if self.missing:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

        return {"Body": io.BytesIO(self.payload)}


class DataModuleTests(unittest.TestCase):
    def test_fetches_dataset_once_and_then_hits_cache(self) -> None:
        client = _FakeS3Client(_fixture_parquet_bytes())

        with tempfile.TemporaryDirectory() as cache_dir:
            with mock.patch.dict(
                "os.environ",
                {
                    "AWS_REGION": "eu-west-1",
                    "DATASET_CACHE_DIR": cache_dir,
                    "S3_BUCKET": "test-bucket",
                    "S3_PREFIX": "dev",
                },
                clear=True,
            ):
                with mock.patch.object(data.boto3, "client", return_value=client):
                    first = data.universe_history()
                    second = data.universe_history()

        self.assertEqual(
            client.calls,
            [("test-bucket", "dev/datasets/universe_history.parquet")],
        )
        pd.testing.assert_frame_equal(first, second)

    def test_missing_dataset_raises_typed_error(self) -> None:
        client = _FakeS3Client(b"", missing=True)

        with tempfile.TemporaryDirectory() as cache_dir:
            with mock.patch.dict(
                "os.environ",
                {
                    "AWS_REGION": "eu-west-1",
                    "DATASET_CACHE_DIR": cache_dir,
                    "S3_BUCKET": "test-bucket",
                },
                clear=True,
            ):
                with mock.patch.object(data.boto3, "client", return_value=client):
                    with self.assertRaises(data.DatasetNotFoundError) as context:
                        data.daily_prices()

        self.assertEqual(context.exception.dataset, "daily_prices")
        self.assertEqual(context.exception.bucket, "test-bucket")
        self.assertEqual(context.exception.key, "datasets/daily_prices.parquet")


if __name__ == "__main__":
    unittest.main()
