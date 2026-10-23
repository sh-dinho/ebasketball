"""Coordinate source ingestion."""

from __future__ import annotations

from ebasketball.database import get_connection
from ebasketball.repositories.matches import upsert_match
from ebasketball.repositories.odds import record_fixture_odds


def save_fixture(
    fixture: dict,
    source: str = "h2hgg",
    *,
    source_event_id: str | None = None,
    source_url: str | None = None,
    source_match_key: str | None = None,
    raw_hash: str | None = None,
) -> dict:
    """
    Persist a fixture, its source mapping, and its odds atomically.
    """
    with get_connection() as conn:
        match_id = upsert_match(
            conn,
            fixture,
            source,
            source_event_id=source_event_id,
            source_url=source_url,
            source_match_key=source_match_key,
            raw_hash=raw_hash,
        )

        observations_inserted = record_fixture_odds(
            conn,
            match_id,
            fixture,
            source,
        )

        return {
            "match_id": match_id,
            "odds_observations_inserted": observations_inserted,
            "source": source,
        }
