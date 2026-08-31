from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from agentgraph_connector_feed.server import app as feed_app


def _event(**overrides: object) -> dict[str, object]:
    event: dict[str, object] = {
        "event_id": str(uuid4()),
        "origin_id": str(uuid4()),
        "occurred_at": datetime.now(UTC).isoformat(),
        "kind": "bookmark",
        "target": {
            "platform": "web",
            "platform_entity_id": "https://example.com",
            "entity_type": "Document",
            "resource_type": "document",
            "url": "https://example.com",
        },
        "bookmarked": True,
    }
    event.update(overrides)
    return event


def test_database_path_defaults_to_agentgraph_config_directory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AGENTGRAPH_FEED_DB", raising=False)

    assert feed_app._database_path() == str(
        Path.home() / ".agentgraph" / "agentgraph-feed-events.db"
    )


def test_database_path_reads_project_local_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("AGENTGRAPH_FEED_DB", raising=False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("AGENTGRAPH_FEED_DB=./feed.db\n")

    assert feed_app._database_path() == "./feed.db"


def test_database_path_environment_overrides_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("AGENTGRAPH_FEED_DB=./feed.db\n")
    monkeypatch.setenv("AGENTGRAPH_FEED_DB", "/tmp/configured-feed.db")

    assert feed_app._database_path() == "/tmp/configured-feed.db"


def test_get_db_creates_missing_database_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "agentgraph" / "agentgraph-feed-events.db"
    monkeypatch.setattr(feed_app, "DB", str(db_path))

    with feed_app.get_db():
        pass

    assert db_path.exists()


def test_events_are_ordered_and_cursor_paginated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(feed_app, "DB", str(tmp_path / "events.db"))
    client = TestClient(feed_app.app)

    first = client.post("/events", json=_event())
    second = client.post("/events", json=_event())

    assert first.json() == {"id": 1, "duplicate": False}
    assert second.json() == {"id": 2, "duplicate": False}
    page = client.get("/events", params={"since": 0, "limit": 1}).json()
    assert [event["sequence"] for event in page["events"]] == [1]
    assert page["next_cursor"] == 1
    assert client.get("/events/tail").json() == {"cursor": 2}


def test_duplicate_event_id_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(feed_app, "DB", str(tmp_path / "events.db"))
    client = TestClient(feed_app.app)
    event = _event()

    assert client.post("/events", json=event).json() == {"id": 1, "duplicate": False}
    assert client.post("/events", json=event).json() == {"id": 1, "duplicate": True}


def test_observation_requires_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(feed_app, "DB", str(tmp_path / "events.db"))
    client = TestClient(feed_app.app)
    event = _event(
        kind="observation",
        target={
            "platform": "gmail",
            "platform_entity_id": "thread-1",
            "entity_type": "Email",
            "resource_type": "thread",
        },
        observation_duration_ms=3000,
    )
    event.pop("bookmarked")

    response = client.post("/events", json=event)

    assert response.status_code == 422
