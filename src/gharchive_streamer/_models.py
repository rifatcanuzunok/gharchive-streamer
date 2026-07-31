from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True, order=True)
class GHTimestamp:
    year: int
    month: int
    day: int
    hour: int

    def __post_init__(self):
        if not 1 <= self.month <= 12:
            raise ValueError(f"Invalid month: {self.month}")
        if not 1 <= self.day <= 31:
            raise ValueError(f"Invalid day: {self.day}")
        if not 0 <= self.hour <= 23:
            raise ValueError(f"Invalid hour: {self.hour}")

    @classmethod
    def from_datetime(cls, dt: datetime) -> GHTimestamp:
        return cls(dt.year, dt.month, dt.day, dt.hour)

    def to_url(self) -> str:
        return f"https://data.gharchive.org/{self.year}-{self.month:02d}-{self.day:02d}-{self.hour}.json.gz"

    def next_hour(self) -> GHTimestamp:
        dt = datetime(self.year, self.month, self.day, self.hour) + timedelta(hours=1)
        return self.from_datetime(dt)


def generate_timestamps(start: datetime, end: datetime) -> Iterator[GHTimestamp]:
    current = GHTimestamp.from_datetime(start)
    end_ts = GHTimestamp.from_datetime(end)

    while current <= end_ts:
        yield current
        current = current.next_hour()
