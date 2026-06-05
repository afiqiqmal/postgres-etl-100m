import csv
import io
import unittest

from postgres_etl_100m.core import (
    TransformStats,
    dirty_event_row,
    transform_event_row,
    transform_rows,
)


class TransformEventRowTest(unittest.TestCase):
    def test_transform_normalizes_row_local_fields(self):
        row = [
            "42",
            "1001",
            "  refund  ",
            "19.95",
            "2026-06-05T12:00:00Z",
            "my",
            "token-abc",
        ]

        self.assertEqual(
            transform_event_row(row),
            [
                "42",
                "1001",
                "refund",
                "19.95",
                "2026-06-05T12:00:00Z",
                "MY",
                "token-abc",
            ],
        )

    def test_transform_rejects_bad_amount(self):
        row = ["42", "1001", "refund", "N/A", "2026-06-05T12:00:00Z", "MY", "token"]

        with self.assertRaisesRegex(ValueError, "bad amount"):
            transform_event_row(row)

    def test_transform_rejects_missing_user_id(self):
        row = ["42", "", "logout", "0.00", "2026-06-05T12:00:00Z", "MY", "token"]

        with self.assertRaisesRegex(ValueError, "missing user_id"):
            transform_event_row(row)

    def test_transform_rejects_wrong_column_count(self):
        with self.assertRaisesRegex(ValueError, "wrong column count"):
            transform_event_row(["too", "short"])


class TransformRowsTest(unittest.TestCase):
    def test_transform_rows_writes_clean_rows_and_rejects_with_reason(self):
        source = io.StringIO(
            "event_id,user_id,event_type,amount,created_at,country,token\n"
            "1,10, login ,1.25,2026-06-05T12:00:00Z,my,tok1\n"
            "2,,logout,0.00,2026-06-05T12:00:01Z,my,tok2\n"
        )
        clean = io.StringIO()
        rejects = io.StringIO()

        stats = transform_rows(source, clean, rejects)

        self.assertEqual(stats, TransformStats(read=2, kept=1, rejected=1))
        self.assertEqual(
            list(csv.reader(io.StringIO(clean.getvalue()))),
            [["1", "10", "login", "1.25", "2026-06-05T12:00:00Z", "MY", "tok1"]],
        )
        self.assertEqual(
            list(csv.reader(io.StringIO(rejects.getvalue()))),
            [["2", "", "logout", "0.00", "2026-06-05T12:00:01Z", "my", "tok2", "missing user_id"]],
        )


class DirtyEventRowTest(unittest.TestCase):
    def test_dirty_event_row_matches_expected_buckets(self):
        clean = ["10", "100", "login", "5.00", "2026-06-05T12:00:00Z", "MY", "token"]

        self.assertEqual(dirty_event_row(clean, 0.001, 10)[3], "N/A")
        self.assertEqual(dirty_event_row(clean, 0.003, 10)[2], "  login  ")
        self.assertEqual(dirty_event_row(clean, 0.003, 10)[5], "my")
        self.assertEqual(dirty_event_row(clean, 0.0045, 10)[1], "")
        self.assertEqual(dirty_event_row(clean, 0.0055, 10)[0], "9")
        self.assertEqual(dirty_event_row(clean, 0.99, 10), clean)


if __name__ == "__main__":
    unittest.main()
