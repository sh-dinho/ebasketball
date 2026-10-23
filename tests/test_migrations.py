"""Migration ordering and source-contract checks."""

from pathlib import Path

MIGRATION_DIR = Path(__file__).resolve().parents[1] / "sql" / "migrations"


def test_migration_files_are_sequential_and_executable():
    migrations = sorted(MIGRATION_DIR.glob("[0-9][0-9][0-9]_*.sql"))
    assert migrations
    assert [path.name.split("_", maxsplit=1)[0] for path in migrations] == [
        f"{number:03d}" for number in range(1, len(migrations) + 1)
    ]

    for migration in migrations:
        sql = migration.read_text(encoding="utf-8").strip()
        executable_sql = "\n".join(
            line for line in sql.splitlines() if not line.lstrip().startswith("--")
        ).strip()
        assert "CREATE TABLE" in executable_sql.upper()
        assert "RERUNNABLE" in sql.upper()
        assert executable_sql.upper().startswith("BEGIN;")
        assert executable_sql.upper().endswith("COMMIT;")
