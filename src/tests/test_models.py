from gharchive_streamer._models import GHTimestamp, generate_timestamps
from datetime import datetime
import pytest


class TestGHTimestamp:
    def test_from_datetime(self):
        dt = datetime(2025, 12, 25, 14)
        ts = GHTimestamp.from_datetime(dt)
        assert ts.year == 2025
        assert ts.month == 12
        assert ts.day == 25
        assert ts.hour == 14

    def test_equality(self):
        ts1 = GHTimestamp(2023, 1, 1, 0)
        ts2 = GHTimestamp(2023, 1, 1, 0)
        ts3 = GHTimestamp(2023, 1, 1, 1)

        assert ts1 == ts2
        assert ts1 != ts3

    def test_ordering(self):
        ts1 = GHTimestamp(2023, 1, 1, 0)
        ts2 = GHTimestamp(2023, 1, 1, 1)
        ts3 = GHTimestamp(2023, 1, 2, 0)

        assert ts1 < ts2
        assert ts2 > ts1
        assert ts1 <= ts2
        assert ts2 >= ts1
        assert ts1 < ts3
        assert ts3 > ts1

    def test_next_hour_normal(self):
        ts = GHTimestamp(2023, 1, 1, 10)
        next_ts = ts.next_hour()

        assert next_ts == GHTimestamp(2023, 1, 1, 11)

    def test_next_hour_end_of_day(self):
        ts = GHTimestamp(2023, 1, 1, 23)
        next_ts = ts.next_hour()

        assert next_ts == GHTimestamp(2023, 1, 2, 0)

    def test_next_hour_end_of_month(self):
        ts = GHTimestamp(2023, 1, 31, 23)
        next_ts = ts.next_hour()

        assert next_ts == GHTimestamp(2023, 2, 1, 0)

    def test_next_hour_end_of_year(self):
        ts = GHTimestamp(2023, 12, 31, 23)
        next_ts = ts.next_hour()

        assert next_ts == GHTimestamp(2024, 1, 1, 0)

    def test_next_hour_leap_year(self):
        ts = GHTimestamp(2020, 2, 29, 23)
        next_ts = ts.next_hour()

        assert next_ts == GHTimestamp(2020, 3, 1, 0)

    def test_to_url(self):
        ts = GHTimestamp(2015, 1, 1, 15)
        expected = "https://data.gharchive.org/2015-01-01-15.json.gz"

        assert ts.to_url() == expected

    def test_invalid_month(self):
        with pytest.raises(ValueError):
            GHTimestamp(2023, 13, 1, 0)

    def test_invalid_hour(self):
        with pytest.raises(ValueError):
            GHTimestamp(2023, 1, 1, 24)


class TestGenerateTimestamps:
    def test_single_hour(self):
        start = datetime(2023, 1, 1, 10, 0)
        end = datetime(2023, 1, 1, 10, 0)
        result = list(generate_timestamps(start, end))
        assert result == [GHTimestamp(2023, 1, 1, 10)]

    def test_multiple_hours(self):
        start = datetime(2023, 1, 1, 22, 0)
        end = datetime(2023, 1, 2, 1, 0)
        result = list(generate_timestamps(start, end))
        expected = [
            GHTimestamp(2023, 1, 1, 22),
            GHTimestamp(2023, 1, 1, 23),
            GHTimestamp(2023, 1, 2, 0),
            GHTimestamp(2023, 1, 2, 1),
        ]
        assert result == expected

    def test_count_correct(self):
        start = datetime(2023, 1, 1, 0, 0)
        end = datetime(2023, 1, 2, 0, 0)
        result = list(generate_timestamps(start, end))
        assert len(result) == 25

    def test_start_greater_than_end(self):
        start = datetime(2023, 1, 2, 0, 0)
        end = datetime(2023, 1, 1, 0, 0)
        result = list(generate_timestamps(start, end))
        assert result == []

    def test_datetime_minute_ignored(self):
        start = datetime(2023, 1, 1, 10, 30)
        end = datetime(2023, 1, 1, 12, 45)
        result = list(generate_timestamps(start, end))
        expected = [
            GHTimestamp(2023, 1, 1, 10),
            GHTimestamp(2023, 1, 1, 11),
            GHTimestamp(2023, 1, 1, 12),
        ]
        assert result == expected
