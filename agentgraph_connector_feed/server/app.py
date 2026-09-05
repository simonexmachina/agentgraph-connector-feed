from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal
from uuid import UUID

from agentgraph.config import get_config_paths
from dotenv import dotenv_values
from fastapi import FastAPI, Query
from pydantic import BaseModel, Field, model_validator

DB_FILENAME = "agentgraph-feed-events.db"
logger = logging.getLogger("uvicorn.error")


def _database_path() -> str:
    """Resolve the database path from the environment or project-local .env."""

    configured_path = os.environ.get("AGENTGRAPH_FEED_DB")
    if not configured_path:
        dotenv_value = dotenv_values(".env").get("AGENTGRAPH_FEED_DB")
        configured_path = dotenv_value if isinstance(dotenv_value, str) else None
    return configured_path or str(get_config_paths()[0] / DB_FILENAME)


DB = _database_path()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    logger.info("AgentGraph Feed database: %s", Path(DB).expanduser().resolve())
    yield


app = FastAPI(title="AgentGraph Feed", lifespan=lifespan)

ResourceType = Literal[
    "channel",
    "dm",
    "document",
    "folder",
    "message",
    "spreadsheet",
    "thread",
    "video",
    "work-item",
]


class MutationTarget(BaseModel):
    platform: str = Field(min_length=1)
    platform_entity_id: str = Field(min_length=1)
    entity_type: str = Field(min_length=1)
    resource_type: ResourceType | None = None
    url: str | None = None


class FeedEventBase(BaseModel):
    event_id: UUID
    origin_id: UUID
    occurred_at: datetime
    target: MutationTarget


class ObservationEvent(FeedEventBase):
    kind: Literal["observation"]
    observation_duration_ms: int = Field(gt=0)
    meta: dict[str, str] | None = None

    @model_validator(mode="after")
    def require_url(self) -> ObservationEvent:
        if not self.target.url:
            raise ValueError("observation events require target.url")
        return self


class BookmarkEvent(FeedEventBase):
    kind: Literal["bookmark"]
    bookmarked: bool


class TombstoneEvent(FeedEventBase):
    kind: Literal["tombstone"]


class EntitySnapshot(BaseModel):
    id: str
    entity_type: str
    platform: str
    platform_entity_id: str
    title: str | None = None
    content: str | None = None
    metadata: dict[str, Any]
    created_at: str
    updated_at: str
    source_created_at: str | None = None
    source_updated_at: str | None = None
    synced_at: str | None = None
    observed_at: str | None = None
    retention_policy: str
    retention_parent_id: str | None = None
    cumulative_observation_duration_ms: int
    bookmarked: bool


class EdgeSnapshot(BaseModel):
    id: str
    edge_type: str
    platform: str
    properties: dict[str, Any]
    source_entity_id: str
    target_entity_id: str
    source_ref: str
    target_ref: str


class UpsertEvent(FeedEventBase):
    kind: Literal["upsert"]
    entity: EntitySnapshot
    edges: list[EdgeSnapshot]


FeedEvent = Annotated[
    ObservationEvent | BookmarkEvent | TombstoneEvent | UpsertEvent,
    Field(discriminator="kind"),
]


def get_db() -> sqlite3.Connection:
    db_path = Path(DB).expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE IF NOT EXISTS feed_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            origin_id TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            kind TEXT NOT NULL,
            payload TEXT NOT NULL
        )"""
    )
    return conn


@app.get("/healthcheck")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/events")
def append(event: FeedEvent) -> dict[str, int | bool]:
    payload = event.model_dump_json()
    with get_db() as conn:
        cursor = conn.execute(
            """INSERT INTO feed_events (
                   event_id, origin_id, occurred_at, kind, payload
               ) VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(event_id) DO NOTHING
               RETURNING id""",
            (
                str(event.event_id),
                str(event.origin_id),
                event.occurred_at.isoformat(),
                event.kind,
                payload,
            ),
        )
        row = cursor.fetchone()
        duplicate = row is None
        if row is None:
            row = conn.execute(
                "SELECT id FROM feed_events WHERE event_id = ?",
                (str(event.event_id),),
            ).fetchone()
        if row is None:
            raise RuntimeError("event insert completed without a stored row")
        return {"id": int(row["id"]), "duplicate": duplicate}


@app.get("/events")
def poll(
    since: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, object]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT id, payload
               FROM feed_events
               WHERE id > ?
               ORDER BY id
               LIMIT ?""",
            (since, limit),
        ).fetchall()
    events = [
        {"sequence": int(row["id"]), **json.loads(row["payload"])} for row in rows
    ]
    next_cursor = int(rows[-1]["id"]) if rows else since
    return {"events": events, "next_cursor": next_cursor}


@app.delete("/events")
def purge(through: int = Query(ge=0)) -> dict[str, int]:
    with get_db() as conn:
        cursor = conn.execute("DELETE FROM feed_events WHERE id <= ?", (through,))
    return {"deleted": max(cursor.rowcount, 0)}


@app.get("/events/tail")
def tail() -> dict[str, int]:
    with get_db() as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(id), 0) AS cursor FROM feed_events"
        ).fetchone()
    return {"cursor": int(row["cursor"]) if row is not None else 0}
