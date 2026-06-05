from __future__ import annotations

import csv
from dataclasses import dataclass
from typing import TextIO


EVENT_HEADER = [
    "event_id",
    "user_id",
    "event_type",
    "amount",
    "created_at",
    "country",
    "token",
]


@dataclass(frozen=True)
class TransformStats:
    read: int = 0
    kept: int = 0
    rejected: int = 0


def transform_event_row(row: list[str]) -> list[str]:
    if len(row) != len(EVENT_HEADER):
        raise ValueError("wrong column count")

    event_id, user_id, event_type, amount, created_at, country, token = row
    event_type = event_type.strip()
    country = country.strip().upper()

    if user_id.strip() == "":
        raise ValueError("missing user_id")

    try:
        int(event_id)
    except ValueError as exc:
        raise ValueError("bad event_id") from exc

    try:
        int(user_id)
    except ValueError as exc:
        raise ValueError("bad user_id") from exc

    try:
        float(amount)
    except ValueError as exc:
        raise ValueError("bad amount") from exc

    return [event_id, user_id, event_type, amount, created_at, country, token]


def transform_rows(
    source: TextIO,
    clean_sink: TextIO,
    reject_sink: TextIO,
    *,
    has_header: bool = True,
) -> TransformStats:
    reader = csv.reader(source)
    clean_writer = csv.writer(clean_sink, lineterminator="\n")
    reject_writer = csv.writer(reject_sink, lineterminator="\n")

    if has_header:
        next(reader, None)

    read = kept = rejected = 0
    for row in reader:
        read += 1
        try:
            clean_writer.writerow(transform_event_row(row))
            kept += 1
        except Exception as exc:
            reject_writer.writerow(row + [str(exc)])
            rejected += 1

    return TransformStats(read=read, kept=kept, rejected=rejected)


def dirty_event_row(row: list[str], bucket: float, row_number: int) -> list[str]:
    dirty = list(row)

    if bucket < 0.002:
        dirty[3] = "N/A"
    elif bucket < 0.004:
        dirty[2] = f"  {dirty[2]}  "
        dirty[5] = dirty[5].lower()
    elif bucket < 0.005:
        dirty[1] = ""
    elif bucket < 0.006:
        dirty[0] = str(row_number - 1 if row_number > 1 else row_number)

    return dirty


def checksum_sql(table_name: str) -> str:
    table = quote_identifier(table_name)
    return (
        "SELECT COUNT(*) AS rows_loaded, "
        "MIN(event_id::bigint) AS min_event_id, "
        "MAX(event_id::bigint) AS max_event_id, "
        "SUM(user_id::bigint) AS user_id_sum "
        f"FROM {table};"
    )


def quote_identifier(name: str) -> str:
    if not name or "\x00" in name:
        raise ValueError("invalid identifier")
    return '"' + name.replace('"', '""') + '"'
