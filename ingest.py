"""Incrementally upsert JSON Lines vote records into DuckDB.

The input schema is inferred from the JSON keys, keeping this small exercise
usable with the supplied vote extract without a dataframe dependency.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import duckdb

SCHEMA = "blog_analysis"
TABLE = "votes"
KEY_CANDIDATES = ("vote_id", "id", "voteId", "voteID")
TIMESTAMP_CANDIDATES = (
    "vote_timestamp", "voted_at", "vote_date", "created_at",
    "timestamp", "date", "datetime",
)


def quote_identifier(identifier: str) -> str:
    """Quote a SQL identifier, including JSON keys that contain punctuation."""
    return '"' + identifier.replace('"', '""') + '"'


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number}: {exc.msg}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"Line {line_number} must contain a JSON object")
            records.append(record)
    return records


def _is_timestamp(value: str) -> bool:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        try:
            date.fromisoformat(value)
            return True
        except ValueError:
            return False


def _duckdb_type(values: Iterable[Any]) -> str:
    non_null = [value for value in values if value is not None]
    if not non_null:
        return "VARCHAR"
    if all(isinstance(value, bool) for value in non_null):
        return "BOOLEAN"
    if all(isinstance(value, int) and not isinstance(value, bool) for value in non_null):
        return "BIGINT"
    if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in non_null):
        return "DOUBLE"
    if all(isinstance(value, str) and _is_timestamp(value) for value in non_null):
        return "TIMESTAMP"
    if all(isinstance(value, str) for value in non_null):
        return "VARCHAR"
    return "JSON"


def _db_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), sort_keys=True)
    if isinstance(value, str) and _is_timestamp(value):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return datetime.combine(date.fromisoformat(value), datetime.min.time())
    return value


def _primary_key(records: list[dict[str, Any]]) -> str:
    keys = set().union(*(record.keys() for record in records))
    for candidate in KEY_CANDIDATES:
        if candidate in keys and all(record.get(candidate) is not None for record in records):
            return candidate
    raise ValueError(
        "Each vote must have a non-null unique identifier named one of "
        f"{', '.join(KEY_CANDIDATES)}"
    )


def ingest_votes(input_path: str | Path, database_path: str | Path = "votes.duckdb") -> int:
    """Upsert records from a JSONL file and return the number of input records."""
    records = read_jsonl(input_path)
    if not records:
        return 0

    key = _primary_key(records)
    columns = list(dict.fromkeys(column for record in records for column in record))
    if not columns:
        return 0

    column_types = {
        column: _duckdb_type(record.get(column) for record in records)
        for column in columns
    }

    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(SCHEMA)}")
        table_name = f"{quote_identifier(SCHEMA)}.{quote_identifier(TABLE)}"
        definitions = ", ".join(
            f"{quote_identifier(column)} {column_types[column]}"
            + (" PRIMARY KEY" if column == key else "")
            for column in columns
        )
        connection.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({definitions})")

        existing = {
            row[1]: row[2]
            for row in connection.execute(
                "SELECT * FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ?",
                [SCHEMA, TABLE],
            ).fetchall()
        }
        if key not in existing:
            raise ValueError(
                f"Existing {SCHEMA}.{TABLE} table has no '{key}' column; "
                "use a consistent identifier field across loads"
            )
        for column in columns:
            if column not in existing:
                connection.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN "
                    f"{quote_identifier(column)} {column_types[column]}"
                )

        quoted_columns = ", ".join(quote_identifier(column) for column in columns)
        placeholders = ", ".join("?" for _ in columns)
        updates = [column for column in columns if column != key]
        conflict_action = (
            "DO UPDATE SET " + ", ".join(
                f"{quote_identifier(column)} = excluded.{quote_identifier(column)}"
                for column in updates
            )
            if updates else "DO NOTHING"
        )
        statement = (
            f"INSERT INTO {table_name} ({quoted_columns}) VALUES ({placeholders}) "
            f"ON CONFLICT ({quote_identifier(key)}) {conflict_action}"
        )
        # A transaction ensures a malformed row cannot leave a partially loaded batch.
        with connection:
            for record in records:
                connection.execute(
                    statement,
                    [_db_value(record.get(column)) for column in columns],
                )
    finally:
        connection.close()
    return len(records)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upsert JSONL vote records into DuckDB.")
    parser.add_argument("input", nargs="?", default=os.getenv("VOTES_JSONL", "votes.jsonl"))
    parser.add_argument("--database", default=os.getenv("VOTES_DATABASE", "votes.duckdb"))
    args = parser.parse_args()
    ingest_votes(args.input, args.database)


if __name__ == "__main__":
    main()
