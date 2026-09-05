# AgentGraph Feed

The AgentGraph Feed package provides both the AgentGraph connector and the shared HTTP
server used to exchange graph mutation events.

## Installation

Install the connector into the same Python environment as AgentGraph:

```sh
uv pip install agentgraph-connector-feed
```

The `0.1.x` connector releases support `agentgraph-server>=0.6.1,<0.7`.

## Configuration

Configure the feed server base URL, then inspect the saved configuration:

```sh
agentgraph connector feed configure http://localhost:8767
agentgraph connector feed status
```

The configured URL must not include `/events` or `/events-feed`. The connector adds `/events`
and `/events/tail` when it publishes and polls respectively. For a deployed server behind a
reverse proxy, configure the URL at which those endpoints are exposed.

The connector publishes new local observations, bookmark changes, and explicit deletions as
best-effort HTTP requests. It polls the feed once per minute. Its first poll starts at the current
feed tail, so existing events are not imported.

Entity upserts are ignored by default. Pass `--publish-upserts` to publish full committed entity
and edge snapshots.

## Server

Run the feed server locally with:

```sh
uv run agentgraph-feed-server
```

The server listens on port `8767` by default. Set `AGENTGRAPH_FEED_SERVER_PORT` to use
another port. The event database defaults to
`<AGENTGRAPH_CONFIG_DIR>/agentgraph-feed-events.db`, where the config directory defaults to
`~/.agentgraph`. Set `AGENTGRAPH_FEED_DB` to override the complete database path.

The server exposes `GET /healthcheck`, `POST /events`, `GET /events`, `GET /events/tail`, and
`DELETE /events?through=<sequence>`. The delete endpoint purges only events at or before the
supplied sequence, preserving events appended while a consumer processes its current page. It is
intended for clients using the server as a one-producer, one-consumer queue.

## Security

The feed is unauthenticated and should only be exposed on a trusted network.

## Development

```sh
uv sync --locked
uv run pytest
uv run pyright
uv run ruff check .
uv run ruff format --check .
```
