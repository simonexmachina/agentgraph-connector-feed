# AgentGraph Feed

The AgentGraph Feed package provides both the AgentGraph connector and the shared HTTP
server used to exchange observations, bookmark changes, and explicit deletions.

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

## Server

Run the feed server locally with:

```sh
agentgraph-feed-server
```

The server listens on port `8767` by default. Set `AGENTGRAPH_FEED_SERVER_PORT` to use
another port. The event database defaults to
`~/.agentgraph/agentgraph-feed-events.db`; set `AGENTGRAPH_FEED_DB` to override it.

The server exposes `GET /healthcheck`, `POST /events`, `GET /events`, and
`GET /events/tail`. The connector and server communicate only through this HTTP API.

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
