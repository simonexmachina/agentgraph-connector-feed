"""Local and container entry point for the AgentGraph Feed server."""

from __future__ import annotations

import os

import uvicorn

DEFAULT_PORT = 8767
PORT_ENV_VAR = "AGENTGRAPH_FEED_SERVER_PORT"


def server_port() -> int:
    """Return the configured HTTP port, validating the environment value."""

    configured_port = os.environ.get(PORT_ENV_VAR)
    if configured_port is None:
        return DEFAULT_PORT

    try:
        port = int(configured_port)
    except ValueError as error:
        raise ValueError(
            f"{PORT_ENV_VAR} must be an integer, got {configured_port!r}"
        ) from error

    if not 1 <= port <= 65535:
        raise ValueError(f"{PORT_ENV_VAR} must be between 1 and 65535, got {port}")
    return port


def main() -> None:
    uvicorn.run(
        "agentgraph_connector_feed.server.app:app",
        host="0.0.0.0",
        port=server_port(),
    )
