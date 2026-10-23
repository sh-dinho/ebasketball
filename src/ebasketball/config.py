"""Centralized application configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_connect_timeout: int

    @classmethod
    def from_env(cls) -> Settings:
        """Load and validate settings from environment variables."""
        password = os.getenv("DB_PASSWORD")

        if not password:
            raise ValueError("DB_PASSWORD is required. Configure it in .env.")

        try:
            port = int(os.getenv("DB_PORT", "5433"))
            timeout = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))
        except ValueError as exc:
            raise ValueError(
                "DB_PORT and DB_CONNECT_TIMEOUT must be integers."
            ) from exc

        if not 1 <= port <= 65535:
            raise ValueError("DB_PORT must be between 1 and 65535.")

        if timeout < 1:
            raise ValueError("DB_CONNECT_TIMEOUT must be positive.")

        return cls(
            db_host=os.getenv("DB_HOST", "localhost"),
            db_port=port,
            db_name=os.getenv("DB_NAME", "ebasket_db"),
            db_user=os.getenv("DB_USER", "postgres"),
            db_password=password,
            db_connect_timeout=timeout,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached application settings."""
    return Settings.from_env()
