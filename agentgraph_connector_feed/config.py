"""Connector-owned feed configuration."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID, uuid4

from agentgraph.config import get_config_paths
from pydantic import BaseModel


class FeedConfig(BaseModel):
    feed_url: str
    origin_id: UUID
    publish_upserts: bool = False

    @classmethod
    def create(
        cls,
        feed_url: str,
        origin_id: UUID | None = None,
        *,
        publish_upserts: bool = False,
    ) -> FeedConfig:
        normalised_url = feed_url.rstrip("/")
        parsed = urlparse(normalised_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Feed URL must be an absolute HTTP(S) URL")
        return cls(
            feed_url=normalised_url,
            origin_id=origin_id or uuid4(),
            publish_upserts=publish_upserts,
        )


def feed_config_path() -> Path:
    return get_config_paths()[0] / "feed.toml"


def load_feed_config() -> FeedConfig | None:
    path = feed_config_path()
    if not path.exists():
        return None
    with path.open("rb") as handle:
        return FeedConfig.model_validate(tomllib.load(handle))


def save_feed_config(config: FeedConfig) -> None:
    path = feed_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    content = (
        f"feed_url = {json.dumps(config.feed_url)}\n"
        f"origin_id = {json.dumps(str(config.origin_id))}\n"
        f"publish_upserts = {str(config.publish_upserts).lower()}\n"
    )
    path.write_text(content)
