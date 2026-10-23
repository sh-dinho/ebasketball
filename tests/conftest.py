"""Shared fixtures for isolated PostgreSQL integration tests."""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_DIR = ROOT / "sql" / "migrations"


@pytest.fixture
def empty_test_connection():
    """Provide an empty isolated schema in the explicitly configured test DB."""
    dsn = os.getenv("EBASKETBALL_TEST_DSN")
    if not dsn:
        pytest.skip("set EBASKETBALL_TEST_DSN to run PostgreSQL integration tests")

    psycopg2 = pytest.importorskip("psycopg2")
    from psycopg2 import sql
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    conn.autocommit = True
    schema_name = f"ebasketball_test_{uuid4().hex}"
    created = False
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_database() AS database_name")
            database_name = cursor.fetchone()["database_name"]
            if not database_name.endswith("_test"):
                pytest.fail(
                    "EBASKETBALL_TEST_DSN must connect to a database "
                    "whose name ends in '_test'."
                )
            cursor.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
            )
            created = True
            cursor.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name))
            )
        yield conn
    finally:
        if created:
            with conn.cursor() as cursor:
                cursor.execute("SET search_path TO public")
                cursor.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema_name)
                    )
                )
        conn.close()


@pytest.fixture
def migrated_connection():
    """Run migrations in a disposable schema inside an explicit test DB."""
    dsn = os.getenv("EBASKETBALL_TEST_DSN")
    if not dsn:
        pytest.skip("set EBASKETBALL_TEST_DSN to run PostgreSQL integration tests")

    psycopg2 = pytest.importorskip("psycopg2")
    from psycopg2 import sql
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(dsn, cursor_factory=RealDictCursor)
    conn.autocommit = True
    schema_name = f"ebasketball_test_{uuid4().hex}"

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT current_database() AS database_name"
            )
            database_name = cursor.fetchone()["database_name"]
            if not database_name.endswith("_test"):
                pytest.fail(
                    "EBASKETBALL_TEST_DSN must connect to a database "
                    "whose name ends in '_test'."
                )

            cursor.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
            )
            cursor.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name))
            )

            migration_files = sorted(MIGRATION_DIR.glob("[0-9][0-9][0-9]_*.sql"))
            expected_prefixes = [
                f"{number:03d}"
                for number in range(1, len(migration_files) + 1)
            ]
            actual_prefixes = [
                path.name.split("_", maxsplit=1)[0]
                for path in migration_files
            ]
            if actual_prefixes != expected_prefixes:
                pytest.fail("Migration files must be sequentially numbered from 001.")

            for migration in migration_files:
                cursor.execute(migration.read_text(encoding="utf-8"))

        yield conn
    finally:
        with conn.cursor() as cursor:
            cursor.execute("SET search_path TO public")
            cursor.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema_name)
                )
            )
        conn.close()
