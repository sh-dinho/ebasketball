"""Unit and isolated PostgreSQL integration tests for the migration runner."""

from __future__ import annotations

from pathlib import Path

import pytest

from ebasketball.migrations import (
    Migration,
    MigrationDriftError,
    MigrationError,
    MigrationSafetyError,
    MigrationStatus,
    _LOCK_KEY,
    _LOCK_NAMESPACE,
    _advisory_lock,
    _render_status,
    _validate_ledger,
    apply_migrations,
    discover_migrations,
    migration_status,
)


def _write_migration(directory: Path, filename: str, body: str = "SELECT 1;") -> None:
    (directory / filename).write_text(
        f"-- test migration\nBEGIN;\n{body}\nCOMMIT;\n", encoding="utf-8"
    )


def test_discovery_sorts_by_numeric_version_and_hashes_exact_contents(tmp_path):
    _write_migration(tmp_path, "12_later.sql")
    _write_migration(tmp_path, "2_earlier.sql")

    found = discover_migrations(tmp_path)

    assert [migration.version for migration in found] == [2, 12]
    assert found[0].filename == "2_earlier.sql"
    assert len(found[0].checksum) == 64
    assert "BEGIN;" not in found[0].sql
    assert "COMMIT;" not in found[0].sql


@pytest.mark.parametrize("filename", ["initial.sql", "x_bad.sql", "0_zero.sql", "1_Bad.sql"])
def test_discovery_rejects_invalid_migration_filename(tmp_path, filename):
    _write_migration(tmp_path, filename)
    with pytest.raises(MigrationError, match="filename|version"):
        discover_migrations(tmp_path)


def test_discovery_rejects_duplicate_numeric_versions(tmp_path):
    _write_migration(tmp_path, "001_first.sql")
    _write_migration(tmp_path, "1_second.sql")
    with pytest.raises(MigrationError, match="Duplicate migration version"):
        discover_migrations(tmp_path)


@pytest.mark.parametrize(
    ("record", "message"),
    [
        ({"filename": "001_renamed.sql", "checksum": "x"}, "renamed"),
        ({"filename": "001_example.sql", "checksum": "changed"}, "modified"),
    ],
)
def test_ledger_drift_is_reported(record, message):
    migration = Migration(1, "001_example.sql", "original", "SELECT 1;")
    with pytest.raises(MigrationDriftError, match=message):
        _validate_ledger([migration], {1: record})


class _SafetyCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, _params=None):
        self.conn.queries.append(query)

    def fetchone(self):
        return {"database_name": "ebasketball_test"}


class _SafetyConnection:
    autocommit = False

    def __init__(self):
        self.queries = []

    def cursor(self):
        return _SafetyCursor(self)


def test_apply_requires_explicit_exact_database_name_before_writes(tmp_path):
    conn = _SafetyConnection()
    with pytest.raises(MigrationSafetyError, match="exact target database name"):
        apply_migrations(conn, tmp_path, confirm_database=None)
    assert conn.queries == ["SELECT current_database() AS database_name"]


def test_status_output_is_readable():
    rendered = _render_status(
        [
            MigrationStatus(1, "001_first.sql", "x", True),
            MigrationStatus(2, "002_second.sql", "y", False),
        ]
    )
    assert "APPLIED 0001 001_first.sql" in rendered
    assert "PENDING 0002 002_second.sql" in rendered


def _db_name(conn) -> str:
    with conn.cursor() as cursor:
        cursor.execute("SELECT current_database() AS database_name")
        return cursor.fetchone()["database_name"]


def test_runner_fresh_install_second_run_and_ledger_consistency(empty_test_connection):
    conn = empty_test_connection
    migrations = discover_migrations()
    assert migration_status(conn) == [
        MigrationStatus(m.version, m.filename, m.checksum, False, None)
        for m in migrations
    ]
    with conn.cursor() as cursor:
        cursor.execute("SELECT to_regclass('schema_migrations') AS ledger_table")
        assert cursor.fetchone()["ledger_table"] is None

    first = apply_migrations(conn, confirm_database=_db_name(conn))
    assert [item.applied for item in first] == [True] * len(migrations)
    second = apply_migrations(conn, confirm_database=_db_name(conn))
    assert [item.applied for item in second] == [True] * len(migrations)
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT version, filename, checksum FROM schema_migrations ORDER BY version"
        )
        rows = cursor.fetchall()
    assert [row["version"] for row in rows] == [m.version for m in migrations]
    assert [row["filename"] for row in rows] == [m.filename for m in migrations]
    assert [row["checksum"].strip() for row in rows] == [m.checksum for m in migrations]


@pytest.mark.parametrize("field", ["filename", "checksum"])
def test_runner_rejects_applied_migration_drift(empty_test_connection, field):
    conn = empty_test_connection
    apply_migrations(conn, confirm_database=_db_name(conn))
    with conn.cursor() as cursor:
        cursor.execute(
            f"UPDATE schema_migrations SET {field} = %s WHERE version = 1",
            ("changed.sql" if field == "filename" else "f" * 64,),
        )
    conn.commit()
    with pytest.raises(MigrationDriftError):
        migration_status(conn)


def test_failed_migration_rolls_back_sql_and_ledger(empty_test_connection, tmp_path):
    _write_migration(
        tmp_path,
        "001_failure.sql",
        "CREATE TABLE rollback_probe (id INTEGER);\n"
        "INSERT INTO rollback_probe VALUES (1);\n"
        "SELECT 1 / 0;",
    )
    conn = empty_test_connection
    with pytest.raises(MigrationError, match="rolled back"):
        apply_migrations(conn, tmp_path, confirm_database=_db_name(conn))
    with conn.cursor() as cursor:
        cursor.execute("SELECT to_regclass('rollback_probe') AS probe")
        assert cursor.fetchone()["probe"] is None
        cursor.execute("SELECT count(*) AS total FROM schema_migrations")
        assert cursor.fetchone()["total"] == 0


def test_advisory_lock_prevents_another_session_from_acquiring_it(
    empty_test_connection,
):
    import os

    psycopg2 = pytest.importorskip("psycopg2")
    from psycopg2.extras import RealDictCursor

    conn = empty_test_connection
    second = psycopg2.connect(
        os.environ["EBASKETBALL_TEST_DSN"], cursor_factory=RealDictCursor
    )
    try:
        with _advisory_lock(conn):
            with second.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_try_advisory_lock(%s, %s) AS acquired",
                    (_LOCK_NAMESPACE, _LOCK_KEY),
                )
                assert cursor.fetchone()["acquired"] is False
        second.rollback()
    finally:
        second.close()
