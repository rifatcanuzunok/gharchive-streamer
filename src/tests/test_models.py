from datetime import datetime, timedelta, timezone

import pytest

from gharchive_streamer._models import GHTimestamp, generate_timestamps


class TestGHTimestamp:
    def test_from_datetime(self):
        dt = datetime(2025, 12, 25, 14)
        ts = GHTimestamp.from_datetime(dt)
        assert ts.year == 2025
        assert ts.month == 12
        assert ts.day == 25
        assert ts.hour == 14

    def test_from_datetime_naive_assumed_utc(self):
        ts = GHTimestamp.from_datetime(datetime(2023, 1, 1, 5))
        assert ts == GHTimestamp(2023, 1, 1, 5)

    def test_from_datetime_utc_aware(self):
        ts = GHTimestamp.from_datetime(
            datetime(2023, 1, 1, 5, tzinfo=timezone.utc)
        )
        assert ts == GHTimestamp(2023, 1, 1, 5)

    def test_from_datetime_non_utc_converted_to_utc(self):
        tz = timezone(timedelta(hours=3))
        ts = GHTimestamp.from_datetime(datetime(2023, 1, 1, 3, tzinfo=tz))
        assert ts == GHTimestamp(2023, 1, 1, 0)

    def test_from_datetime_conversion_crosses_midnight(self):
        tz = timezone(timedelta(hours=3))
        ts = GHTimestamp.from_datetime(datetime(2023, 1, 1, 1, tzinfo=tz))
        assert ts == GHTimestamp(2022, 12, 31, 22)

    def test_from_datetime_negative_offset(self):
        tz = timezone(timedelta(hours=-5))
        ts = GHTimestamp.from_datetime(datetime(2023, 1, 1, 22, tzinfo=tz))
        assert ts == GHTimestamp(2023, 1, 2, 3)

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

    def test_invalid_year(self):
        with pytest.raises(ValueError):
            GHTimestamp(0, 1, 1, 0)

    def test_invalid_day(self):
        with pytest.raises(ValueError):
            GHTimestamp(2023, 1, 32, 0)

    def test_feb_30_invalid(self):
        with pytest.raises(ValueError):
            GHTimestamp(2023, 2, 30, 0)

    def test_apr_31_invalid(self):
        with pytest.raises(ValueError):
            GHTimestamp(2023, 4, 31, 0)

    def test_feb_29_non_leap_year_invalid(self):
        with pytest.raises(ValueError):
            GHTimestamp(2023, 2, 29, 0)

    def test_feb_29_leap_year_valid(self):
        assert GHTimestamp(2020, 2, 29, 23).next_hour() == GHTimestamp(2020, 3, 1, 0)


class TestGenerateTimestamps:
    def test_single_hour(self):
        start = datetime(2023, 1, 1, 10, 0)
        end = datetime(2023, 1, 1, 11, 0)
        result = list(generate_timestamps(start, end))
        assert result == [GHTimestamp(2023, 1, 1, 10)]

    def test_zero_length_range_is_empty(self):
        start = datetime(2023, 1, 1, 10, 0)
        end = datetime(2023, 1, 1, 10, 0)
        result = list(generate_timestamps(start, end))
        assert result == []

    def test_end_is_exclusive(self):
        result = list(
            generate_timestamps(
                datetime(2023, 1, 1, 23, 0), datetime(2023, 1, 2, 0, 0)
            )
        )
        assert result == [GHTimestamp(2023, 1, 1, 23)]

    def test_multiple_hours(self):
        start = datetime(2023, 1, 1, 22, 0)
        end = datetime(2023, 1, 2, 1, 0)
        result = list(generate_timestamps(start, end))
        expected = [
            GHTimestamp(2023, 1, 1, 22),
            GHTimestamp(2023, 1, 1, 23),
            GHTimestamp(2023, 1, 2, 0),
        ]
        assert result == expected

    def test_count_correct(self):
        start = datetime(2023, 1, 1, 0, 0)
        end = datetime(2023, 1, 2, 0, 0)
        result = list(generate_timestamps(start, end))
        assert len(result) == 24

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
