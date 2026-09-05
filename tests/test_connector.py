from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, Self, cast
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import pytest
from agentgraph.connectors.feed import BookmarkMutation, MutationEvent, MutationTarget

from agentgraph_connector_feed import AgentGraphFeedConnector, _apply_event
from agentgraph_connector_feed.config import (
    FeedConfig,
    load_feed_config,
    save_feed_config,
)


class _Response:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


class _Client:
    responses: ClassVar[dict[str, dict[str, Any]]] = {}
    posts: ClassVar[list[tuple[str, dict[str, Any]]]] = []

    def __init__(self, **_: Any) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def get(self, url: str, **_: Any) -> _Response:
        return _Response(self.responses[url])

    async def post(self, url: str, json: dict[str, Any]) -> _Response:
        self.posts.append((url, json))
        return _Response({"id": 1, "duplicate": False})


@pytest.fixture
def config() -> FeedConfig:
    return FeedConfig(
        feed_url="https://feed.example.test",
        origin_id=UUID("00000000-0000-0000-0000-000000000001"),
    )


def test_config_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "feed.toml"
    config = FeedConfig.create(
        "https://feed.example.test/",
        publish_upserts=True,
    )

    with patch("agentgraph_connector_feed.config.feed_config_path", return_value=path):
        save_feed_config(config)
        loaded = load_feed_config()

    assert loaded == config
    assert loaded is not None and loaded.feed_url == "https://feed.example.test"


def test_config_defaults_to_ignoring_upserts(tmp_path: Path) -> None:
    path = tmp_path / "feed.toml"
    path.write_text(
        'feed_url = "https://feed.example.test"\n'
        'origin_id = "00000000-0000-0000-0000-000000000001"\n'
    )

    with patch("agentgraph_connector_feed.config.feed_config_path", return_value=path):
        loaded = load_feed_config()

    assert loaded is not None
    assert loaded.publish_upserts is False


def test_configure_cli_enables_upsert_delivery() -> None:
    origin_id = UUID("00000000-0000-0000-0000-000000000002")

    with patch("agentgraph_connector_feed.save_feed_config") as save_config:
        result = AgentGraphFeedConnector.run_cli_command(
            [
                "configure",
                "https://feed.example.test/",
                "--origin-id",
                str(origin_id),
                "--publish-upserts",
            ]
        )

    saved = save_config.call_args.args[0]
    assert saved == FeedConfig(
        feed_url="https://feed.example.test",
        origin_id=origin_id,
        publish_upserts=True,
    )
    assert result["publish_upserts"] is True


@pytest.mark.asyncio
async def test_publish_adds_stable_origin(config: FeedConfig) -> None:
    _Client.posts = []
    event = BookmarkMutation(
        target=MutationTarget(
            platform="web",
            platform_entity_id="https://example.com",
            entity_type="Document",
            resource_type="document",
            url="https://example.com",
        ),
        bookmarked=True,
    )

    with (
        patch("agentgraph_connector_feed.load_feed_config", return_value=config),
        patch("agentgraph_connector_feed.httpx.AsyncClient", _Client),
    ):
        await AgentGraphFeedConnector().publish_mutation(event)

    assert _Client.posts[0][0] == "https://feed.example.test/events"
    assert _Client.posts[0][1]["origin_id"] == str(config.origin_id)
    assert _Client.posts[0][1]["kind"] == "bookmark"


@pytest.mark.asyncio
async def test_publish_ignores_upsert_mutation() -> None:
    event = cast(MutationEvent, SimpleNamespace(kind="upsert"))

    with (
        patch(
            "agentgraph_connector_feed.load_feed_config",
            return_value=FeedConfig(
                feed_url="https://feed.example.test",
                origin_id=UUID("00000000-0000-0000-0000-000000000001"),
            ),
        ),
        patch("agentgraph_connector_feed.httpx.AsyncClient", _Client),
    ):
        _Client.posts = []
        await AgentGraphFeedConnector().publish_mutation(event)

    assert _Client.posts == []


@pytest.mark.asyncio
async def test_publish_sends_upsert_when_enabled(config: FeedConfig) -> None:
    config.publish_upserts = True
    event = cast(
        MutationEvent,
        SimpleNamespace(
            kind="upsert",
            model_dump=lambda **_: {"kind": "upsert", "entity": {}, "edges": []},
        ),
    )

    with (
        patch("agentgraph_connector_feed.load_feed_config", return_value=config),
        patch("agentgraph_connector_feed.httpx.AsyncClient", _Client),
    ):
        _Client.posts = []
        await AgentGraphFeedConnector().publish_mutation(event)

    assert _Client.posts == [
        (
            "https://feed.example.test/events",
            {
                "kind": "upsert",
                "entity": {},
                "edges": [],
                "origin_id": str(config.origin_id),
            },
        )
    ]


@pytest.mark.asyncio
async def test_first_poll_starts_at_feed_tail(
    config: FeedConfig, caplog: pytest.LogCaptureFixture
) -> None:
    _Client.responses = {"https://feed.example.test/events/tail": {"cursor": 42}}

    caplog.set_level(logging.INFO, logger="agentgraph_connector_feed")
    with (
        patch("agentgraph_connector_feed.load_feed_config", return_value=config),
        patch("agentgraph_connector_feed.httpx.AsyncClient", _Client),
    ):
        batch, cursor = await AgentGraphFeedConnector().poll({})

    assert batch.entities == []
    assert cursor == {"last_event_id": 42}
    assert "Polling feed server at https://feed.example.test" in caplog.messages


@pytest.mark.asyncio
async def test_remote_observation_reuses_server_handler(config: FeedConfig) -> None:
    event_id = uuid4()
    event = {
        "sequence": 7,
        "event_id": str(event_id),
        "origin_id": str(uuid4()),
        "occurred_at": datetime.now(UTC).isoformat(),
        "kind": "observation",
        "target": {
            "platform": "web",
            "platform_entity_id": "https://example.com",
            "entity_type": "Document",
            "resource_type": "document",
            "url": "https://example.com",
        },
        "observation_duration_ms": 3000,
        "meta": {"source": "shared"},
    }

    with patch(
        "agentgraph_connector_feed.record_observation",
        new=AsyncMock(return_value={"status": "accepted"}),
    ) as record:
        await _apply_event(event, config)

    record.assert_awaited_once_with(
        url="https://example.com",
        observation_duration_ms=3000,
        observation_id=str(event_id),
        observed=True,
        meta={"source": "shared"},
    )


@pytest.mark.asyncio
async def test_self_observation_is_not_applied(config: FeedConfig) -> None:
    event = {
        "sequence": 8,
        "event_id": str(uuid4()),
        "origin_id": str(config.origin_id),
        "kind": "observation",
        "target": {"url": "https://example.com"},
        "observation_duration_ms": 3000,
    }

    with patch(
        "agentgraph_connector_feed.record_observation", new=AsyncMock()
    ) as record:
        await _apply_event(event, config)

    record.assert_not_awaited()


@pytest.mark.asyncio
async def test_own_bookmark_is_replayed_for_feed_order(config: FeedConfig) -> None:
    event = {
        "sequence": 9,
        "event_id": str(uuid4()),
        "origin_id": str(config.origin_id),
        "kind": "bookmark",
        "target": {
            "platform": "web",
            "platform_entity_id": "https://example.com",
            "entity_type": "Document",
            "url": "https://example.com",
        },
        "bookmarked": False,
    }

    with patch(
        "agentgraph_connector_feed._apply_bookmark", new=AsyncMock()
    ) as apply_bookmark:
        await _apply_event(event, config)

    apply_bookmark.assert_awaited_once()
