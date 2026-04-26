from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from agent_invest_scripts import write_memory


class FakeBody:
    def __init__(self, payload: str) -> None:
        self._payload = payload.encode("utf-8")

    def read(self) -> bytes:
        return self._payload


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, dict[str, str]] = {}
        self._etag_counter = 0
        self.before_put: Callable[..., None] | None = None

    def seed_object(self, key: str, body: str) -> None:
        self._etag_counter += 1
        self.objects[key] = {"body": body, "etag": f"etag-{self._etag_counter}"}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        del Bucket
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

        payload = self.objects[Key]
        return {"Body": FakeBody(payload["body"]), "ETag": f'"{payload["etag"]}"'}

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        if self.before_put is not None:
            self.before_put(**kwargs)

        key = kwargs["Key"]
        current = self.objects.get(key)
        if_match = kwargs.get("IfMatch")
        if_none_match = kwargs.get("IfNoneMatch")

        if if_none_match == "*" and current is not None:
            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed", "Message": "object exists"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )

        if if_match is not None:
            expected = current["etag"] if current is not None else None
            if expected is None or if_match != f'"{expected}"':
                raise ClientError(
                    {
                        "Error": {
                            "Code": "PreconditionFailed",
                            "Message": "etag mismatch",
                        },
                        "ResponseMetadata": {"HTTPStatusCode": 412},
                    },
                    "PutObject",
                )

        self._etag_counter += 1
        self.objects[key] = {
            "body": kwargs["Body"],
            "etag": f"etag-{self._etag_counter}",
        }
        return {"ETag": f'"etag-{self._etag_counter}"'}


@pytest.fixture
def memory_env() -> dict[str, str]:
    return {
        "S3_BUCKET": "test-bucket",
        "S3_PREFIX": "agent-invest/dev",
        "AWS_REGION": "us-east-1",
    }


def test_write_memory_replace_then_append_round_trips_known_section(
    memory_env: dict[str, str],
) -> None:
    client = FakeS3Client()

    with patch.dict("os.environ", memory_env, clear=True):
        replaced = write_memory.write_memory(
            scope="strategy",
            user_id="user-1",
            strategy_id="strategy-1",
            section="tried",
            mode="replace",
            content="- first attempt",
            s3_client=client,
        )
        appended = write_memory.write_memory(
            scope="strategy",
            user_id="user-1",
            strategy_id="strategy-1",
            section="tried",
            mode="append",
            content="- second attempt",
            s3_client=client,
        )

    key = "agent-invest/dev/users/user-1/strategies/strategy-1/memory.md"
    assert (
        replaced["path"]
        == "s3://test-bucket/agent-invest/dev/users/user-1/strategies/strategy-1/memory.md"
    )
    assert appended["content"] == "- first attempt\n- second attempt"
    assert client.objects[key]["body"] == (
        "## preferences\n\n"
        "## patterns\n\n"
        "## tried\n\n"
        "- first attempt\n"
        "- second attempt\n\n"
        "## decisions\n\n"
        "## open_threads\n\n"
        "## next\n\n"
        "## spec\n"
    )


def test_write_memory_main_rejects_unknown_section_with_json_error(
    capsys: pytest.CaptureFixture[str], memory_env: dict[str, str]
) -> None:
    with patch.dict("os.environ", memory_env, clear=True):
        with pytest.raises(SystemExit) as error:
            write_memory.main(
                [
                    "--scope",
                    "user",
                    "--user",
                    "user-1",
                    "--section",
                    "unknown",
                    "--mode",
                    "append",
                    "--content",
                    "hello",
                ]
            )

    captured = capsys.readouterr()
    assert error.value.code == 1
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "error": {
            "type": "ValueError",
            "message": (
                "Unknown section: unknown. Expected one of: preferences, patterns, "
                "tried, decisions, open_threads, next, spec"
            ),
        }
    }


def test_write_memory_retries_once_after_conditional_conflict(
    memory_env: dict[str, str],
) -> None:
    client = FakeS3Client()
    key = "agent-invest/dev/users/user-1/strategies/strategy-1/memory.md"
    client.seed_object(
        key,
        (
            "## preferences\n\n"
            "## patterns\n\n"
            "## tried\n\n"
            "- baseline\n\n"
            "## decisions\n\n"
            "## open_threads\n\n"
            "## next\n\n"
            "## spec\n"
        ),
    )

    def inject_concurrent_write(**kwargs: Any) -> None:
        del kwargs
        client.before_put = None
        client.seed_object(
            key,
            (
                "## preferences\n\n"
                "## patterns\n\n"
                "## tried\n\n"
                "- baseline\n"
                "- concurrent writer\n\n"
                "## decisions\n\n"
                "## open_threads\n\n"
                "## next\n\n"
                "## spec\n"
            ),
        )
        raise ClientError(
            {
                "Error": {
                    "Code": "PreconditionFailed",
                    "Message": "etag mismatch",
                },
                "ResponseMetadata": {"HTTPStatusCode": 412},
            },
            "PutObject",
        )

    client.before_put = inject_concurrent_write

    with patch.dict("os.environ", memory_env, clear=True):
        result = write_memory.write_memory(
            scope="strategy",
            user_id="user-1",
            strategy_id="strategy-1",
            section="tried",
            mode="append",
            content="- retry winner",
            s3_client=client,
        )

    assert result["content"] == "- baseline\n- concurrent writer\n- retry winner"
    assert client.objects[key]["body"] == (
        "## preferences\n\n"
        "## patterns\n\n"
        "## tried\n\n"
        "- baseline\n"
        "- concurrent writer\n"
        "- retry winner\n\n"
        "## decisions\n\n"
        "## open_threads\n\n"
        "## next\n\n"
        "## spec\n"
    )


def test_write_memory_surfaces_second_conditional_conflict(
    memory_env: dict[str, str],
) -> None:
    client = FakeS3Client()
    key = "agent-invest/dev/users/user-1/profile.md"
    client.seed_object(
        key,
        (
            "## preferences\n\n"
            "- existing\n\n"
            "## patterns\n\n"
            "## tried\n\n"
            "## decisions\n\n"
            "## open_threads\n\n"
            "## next\n\n"
            "## spec\n"
        ),
    )

    attempts = {"count": 0}

    def always_conflict(**kwargs: Any) -> None:
        del kwargs
        attempts["count"] += 1
        raise ClientError(
            {
                "Error": {
                    "Code": "PreconditionFailed",
                    "Message": "etag mismatch",
                },
                "ResponseMetadata": {"HTTPStatusCode": 412},
            },
            "PutObject",
        )

    with patch.dict("os.environ", memory_env, clear=True):
        client.before_put = always_conflict
        with pytest.raises(RuntimeError, match="Concurrent write conflict"):
            write_memory.write_memory(
                scope="user",
                user_id="user-1",
                strategy_id=None,
                section="preferences",
                mode="append",
                content="- new",
                s3_client=client,
            )

    assert attempts["count"] == 2
