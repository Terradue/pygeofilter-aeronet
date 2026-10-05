from __future__ import annotations

import json
from datetime import datetime, timezone
from http import HTTPStatus
from typing import TYPE_CHECKING
from unittest.mock import patch

import httpx
import pytest

from pygeofilter_aeronet import utils

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.mark.parametrize("pretty_print", [False, True])
def test_json_dump_serializes_datetime_to_stdout(
    capsys: pytest.CaptureFixture[str], pretty_print: bool
) -> None:
    document = {"time": datetime(2025, 1, 2, 3, 4, tzinfo=timezone.utc), "count": 2}
    utils.json_dump(document, pretty_print=pretty_print)
    output = capsys.readouterr().out
    assert json.loads(output) == {"time": "2025-01-02T03:04:00+00:00", "count": 2}
    assert ("\n" in output) is pretty_print


@pytest.mark.parametrize(
    "value, expected",
    [(None, ""), (b"", ""), ("", ""), ("café", "café"), ("café".encode(), "café")],
)
def test_decode(value: str | bytes | None, expected: str) -> None:
    assert utils._decode(value) == expected


def test_datetime_serialization_preserves_other_values() -> None:
    value = object()
    assert utils._support_datetime_serialization(value) is value


@pytest.mark.parametrize("body", [b"", "café".encode()])
def test_verbose_client_logs_request_and_successful_response(body: bytes) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        assert request.content == body
        return httpx.Response(HTTPStatus.OK, content=body, headers={"x-result": "yes"})

    with (
        httpx.Client(transport=httpx.MockTransport(respond)) as client,
        patch.object(utils, "logger") as logger,
    ):
        utils.verbose_client(client)
        response = client.post(
            "https://example.test/search", content=body, headers={"x-query": "yes"}
        )

    assert response.content == body
    logger.warning.assert_any_call("POST https://example.test/search")
    logger.warning.assert_any_call("> x-query: yes")
    logger.success.assert_any_call("< 200 OK")
    logger.success.assert_any_call("< x-result: yes")
    if body:
        logger.warning.assert_any_call("café")
        logger.success.assert_any_call("café")
    logger.error.assert_not_called()


def test_verbose_client_can_build_streaming_request() -> None:
    def chunks() -> Iterator[bytes]:
        yield b"streamed body"

    with httpx.Client() as client, patch.object(utils, "logger") as logger:
        utils.verbose_client(client)
        request = client.build_request("POST", "https://example.test", content=chunks())
        logger.warning.assert_any_call("[REQUEST BUILT FROM STREAM, OMISSING]")
        assert request.read() == b"streamed body"


@pytest.mark.parametrize(
    "status",
    [HTTPStatus.MULTIPLE_CHOICES, HTTPStatus.BAD_REQUEST, HTTPStatus.INTERNAL_SERVER_ERROR],
)
def test_verbose_client_logs_and_raises_for_unsuccessful_responses(status: HTTPStatus) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="failure")

    with (
        httpx.Client(transport=httpx.MockTransport(respond)) as client,
        patch.object(utils, "logger") as logger,
    ):
        utils.verbose_client(client)
        with pytest.raises(RuntimeError, match=r"GET https://example\.test/search"):
            client.get("https://example.test/search")
    logger.error.assert_any_call(f"< {status.value} {status.phrase}")
    logger.error.assert_any_call("failure")
    logger.success.assert_not_called()
