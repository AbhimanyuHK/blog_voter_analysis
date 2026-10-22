"""Focused, isolated tests for ingestion and weekly outlier SQL."""
import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from ingest import ingest_votes
from outliers import create_outlier_view


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.database = self.root / "test.duckdb"
        self.input = self.root / "votes.jsonl"

    def tearDown(self):
        self.temp_dir.cleanup()

    def write_records(self, records):
        self.input.write_text(
            "".join(json.dumps(record) + "\n" for record in records),
            encoding="utf-8",
        )

    def test_inserts_and_reruns_without_duplicates(self):
        records = [
            {"vote_id": 1, "blog_id": 10, "vote_timestamp": "2025-01-06T12:00:00"},
            {"vote_id": 2, "blog_id": 10, "vote_timestamp": "2025-01-07T12:00:00"},
        ]
        self.write_records(records)
        self.assertEqual(ingest_votes(self.input, self.database), 2)
        self.assertEqual(ingest_votes(self.input, self.database), 2)
        with duckdb.connect(str(self.database), read_only=True) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM blog_analysis.votes").fetchone()[0],
                2,
            )

    def test_existing_identifier_is_updated(self):
        self.write_records([{
            "vote_id": 7, "blog_id": 10, "vote_timestamp": "2025-01-06T12:00:00"
        }])
        ingest_votes(self.input, self.database)
        self.write_records([{
            "vote_id": 7, "blog_id": 22, "vote_timestamp": "2025-01-13T12:00:00"
        }])
        ingest_votes(self.input, self.database)
        with duckdb.connect(str(self.database), read_only=True) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT blog_id FROM blog_analysis.votes WHERE vote_id = 7"
                ).fetchone()[0],
                22,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM blog_analysis.votes").fetchone()[0],
                1,
            )

    def test_empty_file_is_a_no_op(self):
        self.input.write_text("", encoding="utf-8")
        self.assertEqual(ingest_votes(self.input, self.database), 0)
        self.assertFalse(self.database.exists())


class OutlierTests(unittest.TestCase):
    def test_view_contains_only_weeks_over_twenty_percent_from_mean(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "outliers.duckdb"
            with duckdb.connect(str(database)) as connection:
                connection.execute("CREATE SCHEMA blog_analysis")
                connection.execute("""
                    CREATE TABLE blog_analysis.votes (
                        vote_id BIGINT PRIMARY KEY,
                        vote_timestamp TIMESTAMP NOT NULL
                    )
                """)
                # Weekly totals: 10, 10, 10, 15. Mean is 11.25; only week 4 is >20% away.
                rows = []
                for index, (count, start) in enumerate((
                    (10, "2025-01-06"),
                    (10, "2025-01-13"),
                    (10, "2025-01-20"),
                    (15, "2025-01-27"),
                ), start=1):
                    rows.extend(
                        (index * 100 + offset, start + " 12:00:00")
                        for offset in range(count)
                    )
                connection.executemany(
                    "INSERT INTO blog_analysis.votes VALUES (?, ?)", rows
                )
                create_outlier_view(connection)
                result = connection.execute(
                    "SELECT week_start, total_votes FROM blog_analysis.outlier_weeks"
                ).fetchall()
                expected_week = connection.execute("SELECT DATE '2025-01-27'").fetchone()[0]
                self.assertEqual(result, [(expected_week, 15)])


if __name__ == "__main__":
    unittest.main()
