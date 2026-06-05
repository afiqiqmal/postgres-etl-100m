#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TextIO

from postgres_etl_100m.core import EVENT_HEADER, dirty_event_row


EVENT_TYPES = ["login", "logout", "purchase", "refund", "password_reset", "view"]
COUNTRIES = ["MY", "SG", "ID", "TH", "VN", "PH", "US", "GB"]


def open_output(path: Path) -> TextIO:
    if path.suffix == ".gz":
        return gzip.open(path, "wt", encoding="utf-8", newline="")
    return path.open("w", encoding="utf-8", newline="")


def clean_event_row(row_number: int, rng: random.Random) -> list[str]:
    created_at = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=row_number)
    amount = f"{rng.randint(0, 500_000) / 100:.2f}"
    token = hashlib.sha256(f"{row_number}:{rng.random()}".encode("utf-8")).hexdigest()[:24]

    return [
        str(row_number),
        str(rng.randint(1, max(2, row_number // 3 + 100))),
        EVENT_TYPES[row_number % len(EVENT_TYPES)],
        amount,
        created_at.isoformat().replace("+00:00", "Z"),
        COUNTRIES[row_number % len(COUNTRIES)],
        token,
    ]


def generate(path: Path, rows: int, *, seed: int, dirty: bool) -> None:
    rng = random.Random(seed)
    with open_output(path) as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(EVENT_HEADER)
        for row_number in range(1, rows + 1):
            row = clean_event_row(row_number, rng)
            if dirty:
                row = dirty_event_row(row, rng.random(), row_number)
            writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate deterministic event CSV data.")
    parser.add_argument("output", type=Path)
    parser.add_argument("rows", type=int)
    parser.add_argument("--seed", type=int, default=20260605)
    parser.add_argument("--dirty", action="store_true")
    args = parser.parse_args()

    if args.rows < 1:
        parser.error("rows must be positive")

    generate(args.output, args.rows, seed=args.seed, dirty=args.dirty)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
