from __future__ import annotations


class GHArchiveError(Exception):
    pass


class DataUnavailableError(GHArchiveError):
    pass


class NetworkError(GHArchiveError):
    pass


class DecompressionError(GHArchiveError):
    pass
