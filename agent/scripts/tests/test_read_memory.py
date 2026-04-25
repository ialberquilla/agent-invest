from __future__ import annotations

import io
import json
import unittest
from unittest import mock

from botocore.exceptions import ClientError

from agent_invest_scripts import read_memory


class _FakeS3Client:
    def __init__(self, payload: bytes = b"", *, missing: bool = False) -> None:
        self.payload = payload
        self.missing = missing
        self.calls: list[tuple[str, str]] = []

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, io.BytesIO]:
        self.calls.append((Bucket, Key))

        if self.missing:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

        return {"Body": io.BytesIO(self.payload)}


class ReadMemoryTests(unittest.TestCase):
    def test_reads_user_profile_from_s3(self) -> None:
        client = _FakeS3Client(b"# Preferences\n- weekly rebalance\n")

        with mock.patch.dict(
            "os.environ",
            {
                "AWS_REGION": "eu-west-1",
                "S3_BUCKET": "test-bucket",
                "S3_PREFIX": "agent-invest/dev",
            },
            clear=True,
        ):
            with mock.patch.object(read_memory.boto3, "client", return_value=client):
                payload = read_memory.read_memory(scope="user", user_id="user-1")

        self.assertEqual(
            payload,
            {
                "scope": "user",
                "path": "agent-invest/dev/users/user-1/profile.md",
                "content": "# Preferences\n- weekly rebalance\n",
            },
        )
        self.assertEqual(
            client.calls,
            [("test-bucket", "agent-invest/dev/users/user-1/profile.md")],
        )

    def test_reads_strategy_memory_from_s3(self) -> None:
        client = _FakeS3Client(b"# Tried\n- lookback=90\n")

        with mock.patch.dict(
            "os.environ",
            {
                "AWS_REGION": "eu-west-1",
                "S3_BUCKET": "test-bucket",
            },
            clear=True,
        ):
            with mock.patch.object(read_memory.boto3, "client", return_value=client):
                payload = read_memory.read_memory(
                    scope="strategy",
                    user_id="user-1",
                    strategy_id="strategy-1",
                )

        self.assertEqual(
            payload,
            {
                "scope": "strategy",
                "path": "users/user-1/strategies/strategy-1/memory.md",
                "content": "# Tried\n- lookback=90\n",
            },
        )
        self.assertEqual(
            client.calls,
            [("test-bucket", "users/user-1/strategies/strategy-1/memory.md")],
        )

    def test_missing_memory_returns_empty_content(self) -> None:
        client = _FakeS3Client(missing=True)

        with mock.patch.dict(
            "os.environ",
            {
                "AWS_REGION": "eu-west-1",
                "S3_BUCKET": "test-bucket",
            },
            clear=True,
        ):
            with mock.patch.object(read_memory.boto3, "client", return_value=client):
                payload = read_memory.read_memory(scope="user", user_id="user-1")

        self.assertEqual(payload["content"], "")
        self.assertEqual(payload["path"], "users/user-1/profile.md")

    def test_parse_args_requires_strategy_for_strategy_scope(self) -> None:
        stderr = io.StringIO()

        with mock.patch("sys.stderr", stderr):
            with self.assertRaises(SystemExit) as context:
                read_memory.parse_args(["--scope", "strategy", "--user", "user-1"])

        self.assertEqual(context.exception.code, 2)
        self.assertEqual(
            json.loads(stderr.getvalue()),
            {"error": "--strategy is required when --scope=strategy"},
        )


if __name__ == "__main__":
    unittest.main()
