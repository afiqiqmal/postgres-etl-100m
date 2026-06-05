# PostgreSQL ETL 100M Proof of Concept

This is the companion proof of concept for the Medium draft:

`The 100 Million Row Question: Designing a ~50 GB ETL Pipeline Into PostgreSQL`

It demonstrates the article's core design:

- generate deterministic event-style CSV data
- inject realistic dirty rows
- transform rows in a streaming path
- quarantine rejected rows with reasons
- stream clean rows into PostgreSQL with `COPY FROM STDIN`
- report row counts, throughput and peak process memory

The code uses Python's standard library plus the `psql` CLI. It does not require `psycopg2`.

## Files

- `gen.py` generates plain or gzipped CSV files.
- `etl.py` streams, transforms, rejects bad rows and loads clean rows into PostgreSQL.
- `benchmark.py` drives generation and loading at one or more scales.
- `postgres_etl_100m/core.py` contains the reusable transform logic.
- `tests/test_core.py` covers generator dirt buckets and row transform behavior.

## Create a database

Example with a local PostgreSQL install:

```bash
createdb events
```

If your `psql` binary is not on `PATH`, pass it with `--psql`, for example:

```bash
--psql /Library/PostgreSQL/17/bin/psql
```

## Generate data

```bash
python3 gen.py data/events_1000000.csv.gz 1000000 --dirty
```

## Load one file

```bash
python3 etl.py \
  data/events_1000000.csv.gz \
  events_staging \
  data/rejects_1000000.csv \
  "dbname=events" \
  --create-staging
```

The loader prints JSON:

```json
{
  "rows_read": 1000000,
  "rows_loaded": 997000,
  "rows_rejected": 3000,
  "seconds": 7.0,
  "rows_per_second": 142857.14,
  "peak_rss_mb": 22.0
}
```

Exact values depend on your machine, PostgreSQL version, disk and row shape.

## Run the benchmark driver

```bash
python3 benchmark.py "dbname=events" 1000000 4000000 10000000
```

For the full prompt scale:

```bash
python3 benchmark.py "dbname=events" 100000000
```

That command can generate a large file and run for a long time. Make sure your disk has enough free space.

## Publish as a companion repository

To satisfy the publisher's request, push this folder to GitHub as a small companion repository and link it from the article.

Suggested repository name:

`postgres-etl-100m`

Suggested article wording:

> Full runnable scripts are in the companion repository. The article keeps only the snippets needed to explain the design.
