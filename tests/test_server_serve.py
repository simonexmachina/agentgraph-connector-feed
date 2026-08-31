from __future__ import annotations

import pytest

from agentgraph_connector_feed.server import serve


def test_server_port_defaults_to_8767(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(serve.PORT_ENV_VAR, raising=False)

    assert serve.server_port() == 8767


def test_server_port_reads_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(serve.PORT_ENV_VAR, "9123")

    assert serve.server_port() == 9123


@pytest.mark.parametrize("value", ["invalid", "0", "65536"])
def test_server_port_rejects_invalid_environment_values(
    value: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(serve.PORT_ENV_VAR, value)

    with pytest.raises(ValueError, match=serve.PORT_ENV_VAR):
        serve.server_port()
