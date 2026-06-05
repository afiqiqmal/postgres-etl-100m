#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import resource
import subprocess
import time
from pathlib import Path
from typing import TextIO

from postgres_etl_100m.core import EVENT_HEADER, quote_identifier, transform_event_row


def open_input(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def run_psql(dsn: str, sql: str, *, psql: str) -> None:
    result = subprocess.run(
        [psql, dsn, "-v", "ON_ERROR_STOP=1", "-q", "-c", sql],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())


def create_staging_table(dsn: str, table_name: str, *, psql: str, unlogged: bool) -> None:
    table = quote_identifier(table_name)
    mode = "UNLOGGED " if unlogged else ""
    columns = ", ".join(f"{quote_identifier(column)} text" for column in EVENT_HEADER)
    run_psql(dsn, f"DROP TABLE IF EXISTS {table}; CREATE {mode}TABLE {table} ({columns});", psql=psql)


def copy_stream_to_postgres(
    source_path: Path,
    dsn: str,
    table_name: str,
    rejects_path: Path,
    *,
    psql: str,
    has_header: bool,
) -> dict[str, float | int]:
    table = quote_identifier(table_name)
    command = [psql, dsn, "-v", "ON_ERROR_STOP=1", "-q", "-c", f"COPY {table} FROM STDIN WITH (FORMAT csv)"]

    start = time.perf_counter()
    read = kept = rejected = 0

    with open_input(source_path) as source, rejects_path.open("w", encoding="utf-8", newline="") as reject_file:
        reader = csv.reader(source)
        reject_writer = csv.writer(reject_file, lineterminator="\n")
        if has_header:
            next(reader, None)

        process = subprocess.Popen(
            command,
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert process.stdin is not None
        clean_writer = csv.writer(process.stdin, lineterminator="\n")

        for row in reader:
            read += 1
            try:
                clean_writer.writerow(transform_event_row(row))
                kept += 1
            except Exception as exc:
                reject_writer.writerow(row + [str(exc)])
                rejected += 1

        process.stdin.write("\\.\n")
        stdout, stderr = process.communicate()

    if process.returncode != 0:
        raise RuntimeError(stderr.strip() or stdout.strip())

    elapsed = time.perf_counter() - start
    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    return {
        "rows_read": read,
        "rows_loaded": kept,
        "rows_rejected": rejected,
        "seconds": round(elapsed, 3),
        "rows_per_second": round(kept / elapsed, 2) if elapsed else kept,
        "peak_rss_mb": round(rss_mb, 2),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Transform event CSV rows and stream them into PostgreSQL COPY.")
    parser.add_argument("source", type=Path)
    parser.add_argument("table")
    parser.add_argument("rejects", type=Path)
    parser.add_argument("dsn")
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--no-header", action="store_true")
    parser.add_argument("--create-staging", action="store_true")
    parser.add_argument("--logged", action="store_true", help="Use a logged staging table when --create-staging is set.")
    args = parser.parse_args()

    if args.create_staging:
        create_staging_table(args.dsn, args.table, psql=args.psql, unlogged=not args.logged)

    stats = copy_stream_to_postgres(
        args.source,
        args.dsn,
        args.table,
        args.rejects,
        psql=args.psql,
        has_header=not args.no_header,
    )
    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
