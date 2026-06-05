#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, capture_output=True, check=False)


def run_checked(command: list[str]) -> subprocess.CompletedProcess[str]:
    result = run(command)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result


def psql_value(psql: str, dsn: str, sql: str) -> str:
    result = run_checked([psql, dsn, "-t", "-A", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql])
    return result.stdout.strip()


def generate_dataset(path: Path, rows: int) -> float:
    start = time.perf_counter()
    run_checked([sys.executable, str(ROOT / "gen.py"), str(path), str(rows), "--dirty"])
    return time.perf_counter() - start


def run_loader(psql: str, dsn: str, source: Path, rows: int) -> dict[str, object]:
    table = f"events_staging_{rows}"
    rejects = source.with_suffix(source.suffix + ".rejects.csv")
    result = run_checked(
        [
            sys.executable,
            str(ROOT / "etl.py"),
            str(source),
            table,
            str(rejects),
            dsn,
            "--psql",
            psql,
            "--create-staging",
        ]
    )
    stats = json.loads(result.stdout)
    loaded = psql_value(psql, dsn, f"SELECT COUNT(*) FROM {table};")
    stats["db_rows_loaded"] = int(loaded)
    stats["reject_file"] = str(rejects)
    return stats


def benchmark(psql: str, dsn: str, scales: list[int], workdir: Path) -> list[dict[str, object]]:
    rows = []
    for scale in scales:
        source = workdir / f"events_{scale}.csv.gz"
        generation_seconds = generate_dataset(source, scale)
        stats = run_loader(psql, dsn, source, scale)
        stats["rows_requested"] = scale
        stats["source_file"] = str(source)
        stats["generation_seconds"] = round(generation_seconds, 3)
        rows.append(stats)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the PostgreSQL ETL reference benchmark.")
    parser.add_argument("dsn", help='psql connection string, for example "host=/tmp dbname=events user=loader"')
    parser.add_argument("scales", nargs="+", type=int, help="Row counts to generate and load.")
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--workdir", type=Path)
    args = parser.parse_args()

    if any(scale < 1 for scale in args.scales):
        parser.error("all scales must be positive")

    if args.workdir:
        args.workdir.mkdir(parents=True, exist_ok=True)
        rows = benchmark(args.psql, args.dsn, args.scales, args.workdir)
    else:
        with tempfile.TemporaryDirectory(prefix="postgres-etl-100m-") as tmp:
            rows = benchmark(args.psql, args.dsn, args.scales, Path(tmp))

    print(json.dumps({"results": rows}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
