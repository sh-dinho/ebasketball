"""Run ordered PostgreSQL SQL migrations and inspect their ledger.

From the repository root, use ``python -m ebasketball.migrations status`` to
inspect migration state. Applying requires the exact target database name, for
example ``python -m ebasketball.migrations apply --confirm-database ebasketball_test``.
The confirmation is required even for databases whose names end in ``_test``.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import psycopg2

from ebasketball import database

DEFAULT_MIGRATION_DIR = Path(__file__).resolve().parents[2] / "sql" / "migrations"
_FILENAME = re.compile(r"(?P<version>[0-9]+)_(?P<name>[a-z][a-z0-9_]*)\.sql\Z")
_LOCK_NAMESPACE = 1162037314
_LOCK_KEY = 1
# Exact checksum of the short-lived working-tree 001 variant that removed the
# legacy observation objects. Migration 003 restores them. Accept this known
# ledger value without rewriting it so installations that recorded that file
# can advance safely; all other checksum drift remains an error.
_COMPATIBLE_LEGACY_CHECKSUMS = {
    1: frozenset({"1ea74cc77b567d8e773aa9c8e604d65a3624de5762487eb400d55839e3085c1c"}),
}


class MigrationError(RuntimeError):
    """Base class for migration errors suitable for CLI display."""


class MigrationSafetyError(MigrationError):
    """The caller did not confirm the database being modified."""


class MigrationDriftError(MigrationError):
    """A recorded migration no longer matches its source file."""


class MigrationExecutionError(MigrationError):
    """A migration failed and its transaction was rolled back."""


@dataclass(frozen=True)
class Migration:
    version: int
    filename: str
    checksum: str
    sql: str


@dataclass(frozen=True)
class MigrationStatus:
    version: int
    filename: str
    checksum: str
    applied: bool
    applied_at: object | None = None


def _transaction_body(sql_text: str, filename: str) -> str:
    """Remove the migration file's outer transaction markers.

    The runner owns the transaction so the migration and ledger write commit
    atomically. Existing files use standalone BEGIN; and COMMIT; lines.
    """
    lines = sql_text.splitlines()
    begins = [i for i, line in enumerate(lines) if line.strip().upper() == "BEGIN;"]
    commits = [i for i, line in enumerate(lines) if line.strip().upper() == "COMMIT;"]
    if len(begins) != 1 or len(commits) != 1 or begins[0] >= commits[0]:
        raise MigrationError(
            f"{filename} must contain exactly one outer BEGIN; and COMMIT; line."
        )
    del lines[commits[0]]
    del lines[begins[0]]
    return "\n".join(lines)


def discover_migrations(directory: Path = DEFAULT_MIGRATION_DIR) -> list[Migration]:
    """Read and validate numbered SQL migrations in ascending version order."""
    if not directory.is_dir():
        raise MigrationError(f"Migration directory does not exist: {directory}")

    migrations: list[Migration] = []
    versions: dict[int, str] = {}
    for path in sorted(directory.iterdir()):
        if not path.is_file() or path.suffix.lower() != ".sql":
            continue
        match = _FILENAME.fullmatch(path.name)
        if not match:
            raise MigrationError(
                f"Invalid migration filename {path.name!r}; expected "
                "<number>_<lowercase_name>.sql."
            )
        version = int(match.group("version"))
        if version < 1:
            raise MigrationError(f"Migration version must be positive: {path.name}")
        if version in versions:
            raise MigrationError(
                f"Duplicate migration version {version}: {versions[version]} "
                f"and {path.name}."
            )
        versions[version] = path.name
        contents = path.read_bytes()
        try:
            sql_text = contents.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MigrationError(f"Migration {path.name} is not valid UTF-8.") from exc
        migrations.append(
            Migration(
                version=version,
                filename=path.name,
                checksum=hashlib.sha256(contents).hexdigest(),
                sql=_transaction_body(sql_text, path.name),
            )
        )
    return sorted(migrations, key=lambda migration: migration.version)


def _database_name(conn) -> str:
    with conn.cursor() as cursor:
        cursor.execute("SELECT current_database() AS database_name")
        row = cursor.fetchone()
    return row["database_name"]


def _ledger_exists(cursor) -> bool:
    cursor.execute("SELECT to_regclass('schema_migrations') AS ledger_table")
    return cursor.fetchone()["ledger_table"] is not None


def _read_ledger(cursor) -> dict[int, dict[str, object]]:
    if not _ledger_exists(cursor):
        return {}
    cursor.execute(
        "SELECT version, filename, checksum, applied_at "
        "FROM schema_migrations ORDER BY version"
    )
    return {int(row["version"]): dict(row) for row in cursor.fetchall()}


def _validate_ledger(
    migrations: Sequence[Migration], applied: dict[int, dict[str, object]]
) -> None:
    by_version = {migration.version: migration for migration in migrations}
    for version, record in applied.items():
        migration = by_version.get(version)
        if migration is None:
            raise MigrationDriftError(
                f"Ledger contains migration version {version}, but no matching file exists."
            )
        if record["filename"] != migration.filename:
            raise MigrationDriftError(
                f"Applied migration {version} was renamed: ledger has "
                f"{record['filename']!r}, current file is {migration.filename!r}."
            )
        if record["checksum"] != migration.checksum and record[
            "checksum"
        ] not in _COMPATIBLE_LEGACY_CHECKSUMS.get(version, frozenset()):
            raise MigrationDriftError(
                f"Applied migration {version} ({migration.filename}) was modified; "
                "restore the applied file or add a forward migration."
            )


def migration_status(
    conn, directory: Path = DEFAULT_MIGRATION_DIR
) -> list[MigrationStatus]:
    """Return migration state using SELECTs only; do not create the ledger."""
    migrations = discover_migrations(directory)
    with conn.cursor() as cursor:
        applied = _read_ledger(cursor)
    _validate_ledger(migrations, applied)
    return [
        MigrationStatus(
            version=migration.version,
            filename=migration.filename,
            checksum=migration.checksum,
            applied=migration.version in applied,
            applied_at=applied.get(migration.version, {}).get("applied_at"),
        )
        for migration in migrations
    ]


@contextmanager
def _advisory_lock(conn) -> Iterator[None]:
    """Hold a session lock across all migration transactions."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_lock(%s, %s)", (_LOCK_NAMESPACE, _LOCK_KEY))
    conn.commit()
    try:
        yield
    finally:
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_unlock(%s, %s)",
                    (_LOCK_NAMESPACE, _LOCK_KEY),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def _create_ledger(conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY CHECK (version > 0),
                filename TEXT NOT NULL,
                checksum CHAR(64) NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    conn.commit()


def apply_migrations(
    conn,
    directory: Path = DEFAULT_MIGRATION_DIR,
    *,
    confirm_database: str | None = None,
) -> list[MigrationStatus]:
    """Apply pending migrations, requiring exact target database confirmation."""
    target_database = _database_name(conn)
    if confirm_database != target_database:
        raise MigrationSafetyError(
            "Applying migrations requires --confirm-database with the exact "
            f"target database name ({target_database!r})."
        )

    migrations = discover_migrations(directory)
    old_autocommit = conn.autocommit
    if old_autocommit:
        conn.autocommit = False
    try:
        with _advisory_lock(conn):
            _create_ledger(conn)
            with conn.cursor() as cursor:
                applied = _read_ledger(cursor)
            _validate_ledger(migrations, applied)
            for migration in migrations:
                if migration.version in applied:
                    continue
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(migration.sql)
                        cursor.execute(
                            "INSERT INTO schema_migrations "
                            "(version, filename, checksum) VALUES (%s, %s, %s)",
                            (migration.version, migration.filename, migration.checksum),
                        )
                    conn.commit()
                except Exception as exc:
                    conn.rollback()
                    sqlstate = getattr(exc, "pgcode", None)
                    detail = f" (PostgreSQL SQLSTATE {sqlstate})" if sqlstate else ""
                    raise MigrationExecutionError(
                        f"Migration {migration.version} ({migration.filename}) failed; "
                        f"its SQL and ledger write were rolled back{detail}."
                    ) from exc
                applied[migration.version] = {
                    "filename": migration.filename,
                    "checksum": migration.checksum,
                }
        statuses = migration_status(conn, directory)
        conn.commit()
        return statuses
    finally:
        if old_autocommit:
            # Leave the connection idle before restoring its original mode,
            # including if a status query raised after the migration lock.
            conn.rollback()
            conn.autocommit = True


def _render_status(statuses: Sequence[MigrationStatus]) -> str:
    if not statuses:
        return "No migrations found."
    return "\n".join(
        f"{('APPLIED' if item.applied else 'PENDING'):7} "
        f"{item.version:04d} {item.filename}"
        for item in statuses
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manage eBasketball SQL migrations.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("status", "apply"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument(
            "--directory",
            type=Path,
            default=DEFAULT_MIGRATION_DIR,
            help="directory containing numbered SQL migrations",
        )
        if command == "apply":
            subparser.add_argument(
                "--confirm-database",
                required=True,
                metavar="NAME",
                help="exact database name to authorize for migration writes",
            )
    args = parser.parse_args(argv)
    conn = None
    try:
        conn = database.connect()
        if args.command == "status":
            print(_render_status(migration_status(conn, args.directory)))
        else:
            statuses = apply_migrations(
                conn, args.directory, confirm_database=args.confirm_database
            )
            print(_render_status(statuses))
        return 0
    except MigrationError as exc:
        print(f"Migration error: {exc}", file=sys.stderr)
        return 1
    except psycopg2.Error:
        # Database driver errors can contain connection details; do not echo them.
        print("Migration command failed due to a database error.", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"Migration configuration error: {exc}", file=sys.stderr)
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
