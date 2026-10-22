"""Create and print the weekly vote-count outlier view."""
from __future__ import annotations

import argparse
import os

import duckdb

SCHEMA = "blog_analysis"
TABLE = "votes"
VIEW = "outlier_weeks"
DATE_CANDIDATES = (
    "vote_timestamp", "voted_at", "vote_date", "created_at",
    "timestamp", "date", "datetime",
)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def create_outlier_view(connection: duckdb.DuckDBPyConnection) -> None:
    """Recreate a view of weeks >20% above/below the mean weekly vote count."""
    connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
    columns = {
        row[0]
        for row in connection.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ?",
            [SCHEMA, TABLE],
        ).fetchall()
    }
    if not columns:
        raise ValueError(f"Required table {SCHEMA}.{TABLE} does not exist or has no columns")
    date_column = next((name for name in DATE_CANDIDATES if name in columns), None)
    if date_column is None:
        raise ValueError(
            "Could not identify a vote date column. Expected one of: "
            + ", ".join(DATE_CANDIDATES)
        )

    date_expr = f"CAST({quote_identifier(date_column)} AS TIMESTAMP)"
    connection.execute(f'DROP VIEW IF EXISTS "{SCHEMA}"."{VIEW}"')
    connection.execute(f"""
        CREATE VIEW "{SCHEMA}"."{VIEW}" AS
        WITH weekly_totals AS (
            SELECT
                CAST(DATE_TRUNC('week', {date_expr}) AS DATE) AS week_start,
                COUNT(*)::BIGINT AS total_votes
            FROM "{SCHEMA}"."{TABLE}"
            WHERE {quote_identifier(date_column)} IS NOT NULL
            GROUP BY 1
        ),
        weekly_average AS (
            SELECT AVG(total_votes)::DOUBLE AS average_weekly_votes
            FROM weekly_totals
        )
        SELECT
            w.week_start,
            w.total_votes,
            a.average_weekly_votes,
            ((w.total_votes - a.average_weekly_votes)
                / NULLIF(a.average_weekly_votes, 0)) * 100.0 AS deviation_percent
        FROM weekly_totals AS w
        CROSS JOIN weekly_average AS a
        WHERE ABS(w.total_votes - a.average_weekly_votes)
            > a.average_weekly_votes * 0.20
    """)


def main() -> None:
    parser = argparse.ArgumentParser(description="Recreate and print weekly vote outliers.")
    parser.add_argument("--database", default=os.getenv("VOTES_DATABASE", "votes.duckdb"))
    args = parser.parse_args()
    with duckdb.connect(args.database) as connection:
        create_outlier_view(connection)
        for row in connection.execute(
            'SELECT * FROM "blog_analysis"."outlier_weeks" ORDER BY week_start'
        ).fetchall():
            print(row)


if __name__ == "__main__":
    main()
