"""Live providers remain unavailable until their official API contracts verify."""

from __future__ import annotations

import json

import pytest

from ebasketball.sources.live import LiveProviderUnavailable, require_live_provider
from scripts import run_backfill, run_incremental


@pytest.mark.parametrize("provider", ["h2hgg", "aceodds"])
def test_live_provider_factory_fails_closed_with_documentation_gap(provider):
    with pytest.raises(
        LiveProviderUnavailable, match="Live collection.*disabled"
    ) as exc:
        require_live_provider(provider)

    assert "official" in str(exc.value).casefold()
    assert "--input" in str(exc.value)


@pytest.mark.parametrize(
    ("main", "arguments"),
    [
        (run_backfill.main, ["--source", "h2hgg"]),
        (run_incremental.main, ["--source", "aceodds"]),
    ],
)
def test_live_cli_without_input_reports_disabled_provider_and_does_not_persist(
    main, arguments, capsys
):
    assert main(arguments) == 3
    captured = capsys.readouterr()
    payload = json.loads(captured.err)
    assert "LiveProviderUnavailable" in payload["error"]
    assert "disabled" in payload["error"]
    assert captured.out == ""


def test_unknown_live_provider_is_rejected():
    with pytest.raises(ValueError, match="Unsupported"):
        require_live_provider("unknown")
