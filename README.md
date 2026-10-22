# Blog Voter Analysis

A small, on-demand votes pipeline built with Python, DuckDB and SQL. The ingestion and outlier steps are deliberately independent.

## Requirements

- Python 3.10–3.12
- Poetry
- DuckDB (installed by Poetry)

## Run

Place the vote extract in JSON Lines format (one JSON object per line) at `votes.jsonl`. Each record needs a stable non-null identifier named `vote_id`, `id`, `voteId`, or `voteID`, and a date/time field for outlier analysis named `vote_timestamp`, `voted_at`, `vote_date`, `created_at`, `timestamp`, `date`, or `datetime`.

```bash
poetry install
poetry run python ingest.py votes.jsonl --database votes.duckdb
poetry run python outliers.py --database votes.duckdb
poetry run pytest
```

The database path can also be set with `VOTES_DATABASE`; ingestion's default input can be set with `VOTES_JSONL`. The scripts have no dependency on each other: ingestion does not create or refresh the outlier view, and outlier calculation assumes the table already exists.

## Design decisions

- **Schema:** records are stored in `blog_analysis.votes`. Column types are inferred conservatively from JSON values: integers use `BIGINT`, numeric measures use `DOUBLE`, timestamps use `TIMESTAMP`, booleans use `BOOLEAN`, and other values use `VARCHAR` or `JSON`. This is an OLAP-friendly table rather than a dataframe.
- **Incremental loading:** the selected identifier is the primary key. DuckDB's `INSERT … ON CONFLICT … DO UPDATE` makes each run an idempotent upsert: a repeat does not create duplicates and a changed record replaces its prior values. New JSON keys are added as columns. Each batch runs in a transaction.
- **Outlier definition:** first count records per Monday-starting calendar week, then calculate the unweighted mean of those weekly counts. A week is an outlier when its absolute difference from that mean is strictly greater than 20% of the mean. Weeks with no votes are not generated as zero-count weeks because the dataset contains no explicit calendar spine; weeks with a null date are excluded.
- **Output:** `outliers.py` recreates `blog_analysis.outlier_weeks` and prints the view rows only (no status messages).

## Tests

The tests create temporary databases and inputs, so they are self-contained and can be run individually or in any order. They cover insert, repeat-run deduplication, update-on-conflict, empty input, and the SQL outlier threshold. The assignment-provided `exercise-tests/` directory is intentionally not modified.

## AI tool disclosure

OpenAI ChatGPT was used as a coding and review assistant to draft the initial implementation, test cases, SQL, and documentation. The author should review every line, run the checks in the target exercise environment, and be prepared to explain and modify the solution during the interview.
