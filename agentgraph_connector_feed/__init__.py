"""AgentGraph connector for a shared mutation feed."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, ClassVar, cast
from uuid import UUID

import httpx
from agentgraph.connectors.base import EntityBatch, FetchPolicy, ResourceType
from agentgraph.connectors.feed import (
    FeedConnector,
    MutationEvent,
    suppress_feed_notifications,
)
from agentgraph.core.context import get_backend
from agentgraph.graph.bookmark import bookmark_url, set_entity_bookmark
from agentgraph.graph.delete import delete_platform_entity
from agentgraph.server.observation import record_observation

from agentgraph_connector_feed.config import (
    FeedConfig,
    load_feed_config,
    save_feed_config,
)

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT = httpx.Timeout(5.0)
_POLL_LIMIT = 100


class AgentGraphFeedConnector(FeedConnector):
    source: ClassVar[str] = "feed"
    fetch_policy: ClassVar[FetchPolicy] = FetchPolicy(stale_after_seconds=60)
    poll_interval: ClassVar[timedelta | None] = timedelta(minutes=1)
    appears_in_auth_status: ClassVar[bool] = False
    auth_description: ClassVar[str | None] = (
        "Shares local graph mutations through an AgentGraph feed server."
    )

    def can_handle(self, url: str) -> bool:
        _ = url
        return False

    async def fetch(
        self,
        resource_type: ResourceType,
        resource_id: str,
        meta: dict[str, str] | None = None,
        account_id: str | None = None,
    ) -> EntityBatch:
        _ = (resource_type, resource_id, meta, account_id)
        return EntityBatch()

    async def publish_mutation(self, event: MutationEvent) -> None:
        config = load_feed_config()
        if config is None or (event.kind == "upsert" and not config.publish_upserts):
            return
        payload = event.model_dump(mode="json")
        payload["origin_id"] = str(config.origin_id)
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT, trust_env=False) as client:
            response = await client.post(f"{config.feed_url}/events", json=payload)
            response.raise_for_status()

    async def poll(
        self,
        cursor: dict[str, Any],
        account_id: str | None = None,
    ) -> tuple[EntityBatch, dict[str, Any]]:
        _ = account_id
        config = load_feed_config()
        if config is None:
            return EntityBatch(), cursor

        logger.info("Polling feed server at %s", config.feed_url)
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT, trust_env=False) as client:
            if "last_event_id" not in cursor:
                response = await client.get(f"{config.feed_url}/events/tail")
                response.raise_for_status()
                tail_payload = cast(dict[str, Any], response.json())
                return EntityBatch(), {"last_event_id": int(tail_payload["cursor"])}

            since = int(cursor["last_event_id"])
            response = await client.get(
                f"{config.feed_url}/events",
                params={"since": since, "limit": _POLL_LIMIT},
            )
            response.raise_for_status()
            payload = cast(dict[str, Any], response.json())
            events = cast(list[dict[str, Any]], payload.get("events", []))

            next_cursor = since
            for event in events:
                await _apply_event(event, config)
                next_cursor = int(event["sequence"])
            return EntityBatch(), {"last_event_id": next_cursor}

    @classmethod
    def run_cli_command(cls, args: list[str]) -> dict[str, Any]:
        if not args or args[0] in {"--help", "help"}:
            raise ValueError(cls.cli_help())
        if args[0] == "status":
            config = load_feed_config()
            return {
                "configured": config is not None,
                "feed_url": config.feed_url if config else None,
                "origin_id": str(config.origin_id) if config else None,
                "publish_upserts": config.publish_upserts if config else None,
            }
        if args[0] != "configure":
            raise ValueError(
                f"Unknown feed connector command {args[0]!r}\n{cls.cli_help()}"
            )
        config = _parse_configure_args(args[1:])
        save_feed_config(config)
        return {
            "configured": True,
            "feed_url": config.feed_url,
            "origin_id": str(config.origin_id),
            "publish_upserts": config.publish_upserts,
        }

    @classmethod
    def cli_help(cls) -> str:
        return (
            "Usage: agentgraph connector feed configure <feed-url> "
            "[--origin-id <uuid>] [--publish-upserts]\n"
            "   or: agentgraph connector feed status"
        )


async def _apply_event(event: dict[str, Any], config: FeedConfig) -> None:
    kind = event.get("kind")
    if kind == "upsert":
        return
    event_id = str(event["event_id"])
    origin_id = UUID(str(event["origin_id"]))
    target = cast(dict[str, Any], event["target"])

    if kind == "observation":
        if origin_id == config.origin_id:
            return
        url = target.get("url")
        if not isinstance(url, str) or not url:
            raise ValueError(f"Observation event {event_id} has no target URL")
        meta_value = event.get("meta")
        meta = (
            cast(dict[str, str], meta_value) if isinstance(meta_value, dict) else None
        )
        with suppress_feed_notifications():
            result = await record_observation(
                url=url,
                observation_duration_ms=int(event["observation_duration_ms"]),
                observation_id=event_id,
                observed=True,
                meta=meta,
            )
        if result.get("status") == "error":
            raise RuntimeError(
                f"AgentGraph failed to apply observation event {event_id}"
            )
        return

    platform = str(target["platform"])
    platform_entity_id = str(target["platform_entity_id"])
    with suppress_feed_notifications():
        if kind == "bookmark":
            await _apply_bookmark(
                platform=platform,
                platform_entity_id=platform_entity_id,
                entity_type=str(target["entity_type"]),
                url=target.get("url") if isinstance(target.get("url"), str) else None,
                bookmarked=bool(event["bookmarked"]),
            )
            return
        if kind == "tombstone":
            await delete_platform_entity(platform, platform_entity_id)
            return
    raise ValueError(f"Unsupported feed event kind {kind!r}")


async def _apply_bookmark(
    *,
    platform: str,
    platform_entity_id: str,
    entity_type: str,
    url: str | None,
    bookmarked: bool,
) -> None:
    backend = get_backend()
    entity = await backend.get_entity_by_platform(platform, platform_entity_id)
    if entity is None and not bookmarked:
        return
    if entity is None and url is not None:
        try:
            await bookmark_url(url)
        except Exception:  # noqa: BLE001 - remote connector failures should create a stub.
            logger.info("Could not fetch remote bookmark %s; creating a stub", url)
        entity = await backend.get_entity_by_platform(platform, platform_entity_id)
    if entity is None:
        entity_id = await backend.upsert_stub_entity(
            entity_type, platform, platform_entity_id
        )
    else:
        entity_id = str(entity["id"])
    await set_entity_bookmark(entity_id, bookmarked)


def _parse_configure_args(args: list[str]) -> FeedConfig:
    if not args:
        raise ValueError(AgentGraphFeedConnector.cli_help())
    feed_url = args[0]
    origin_id: UUID | None = None
    publish_upserts = False
    index = 1
    while index < len(args):
        if args[index] == "--publish-upserts":
            publish_upserts = True
            index += 1
            continue
        if args[index] != "--origin-id" or index + 1 >= len(args):
            raise ValueError(AgentGraphFeedConnector.cli_help())
        origin_id = UUID(args[index + 1])
        index += 2
    return FeedConfig.create(
        feed_url=feed_url,
        origin_id=origin_id,
        publish_upserts=publish_upserts,
    )


__all__ = ["AgentGraphFeedConnector"]
