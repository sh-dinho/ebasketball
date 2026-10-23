"""Live-provider adapter availability.

This module intentionally does not make HTTP requests. Neither configured
source currently has a verified official API contract for this application.
Keeping the failure explicit prevents a web page or third-party wrapper from
being mistaken for a supported data API.
"""

from __future__ import annotations


class LiveProviderUnavailable(RuntimeError):
    """Raised when a requested provider has no verified live API adapter."""


_PROVIDER_DETAILS = {
    "h2hgg": (
        "No currently reachable, authorized H2HGG data interface is verified. "
        "Parse.bot documented a third-party schedule endpoint, but H2HGG has "
        "no official API and Parse's terms leave source-site permissions to "
        "the caller; the listing URL currently returns 404."
    ),
    "aceodds": (
        "AceOdds publishes a public streaming schedule page, but no official "
        "data API, authentication method, pagination, or historical retention "
        "documentation or permission to automate collection has been verified."
    ),
}


def require_live_provider(name: str) -> None:
    """Fail clearly instead of attempting an undocumented provider request."""
    provider = name.strip().casefold()
    if provider not in _PROVIDER_DETAILS:
        raise ValueError(f"Unsupported live provider: {name!r}")
    raise LiveProviderUnavailable(
        f"Live collection for {provider} is disabled. {_PROVIDER_DETAILS[provider]} "
        "Use --input with an authorized JSON/CSV export, or provide verified "
        "provider documentation, source-data permissions, and access credentials "
        "before enabling a live adapter."
    )
