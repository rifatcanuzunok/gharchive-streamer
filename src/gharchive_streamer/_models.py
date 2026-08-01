from __future__ import annotations

from calendar import monthrange
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True, order=True)
class GHTimestamp:
    year: int
    month: int
    day: int
    hour: int

    def __post_init__(self):
        if not 1 <= self.year <= 9999:
            raise ValueError(f"Invalid year: {self.year}")
        if not 1 <= self.month <= 12:
            raise ValueError(f"Invalid month: {self.month}")
        if not 0 <= self.hour <= 23:
            raise ValueError(f"Invalid hour: {self.hour}")
        days_in_month = monthrange(self.year, self.month)[1]
        if not 1 <= self.day <= days_in_month:
            raise ValueError(
                f"Invalid day: {self.day} for {self.year}-{self.month:02d} "
                f"(month has {days_in_month} days)"
            )

    @classmethod
    def from_datetime(cls, dt: datetime) -> GHTimestamp:
        # GH Archive hours are UTC. Naive datetimes are assumed to be UTC;
        # aware datetimes are converted to UTC so the hour field is always right.
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        return cls(dt.year, dt.month, dt.day, dt.hour)

    def to_url(self) -> str:
        return f"https://data.gharchive.org/{self.year}-{self.month:02d}-{self.day:02d}-{self.hour}.json.gz"

    def next_hour(self) -> GHTimestamp:
        dt = datetime(  # noqa: DTZ001 - GH Archive hours are timezone-free UTC
            self.year, self.month, self.day, self.hour
        ) + timedelta(hours=1)
        return self.from_datetime(dt)


def generate_timestamps(start: datetime, end: datetime) -> Iterator[GHTimestamp]:
    current = GHTimestamp.from_datetime(start)
    last = GHTimestamp.from_datetime(end - timedelta(microseconds=1))

    while current <= last:
        yield current
        current = current.next_hour()
