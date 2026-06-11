"""Unit tests for the usage (Activity Events) date-window helpers."""

from datetime import date

from src.usage import (
    ACTIVITY_EVENTS_MAX_RETENTION_DAYS,
    day_bounds,
    get_dates_to_fetch,
)


class TestGetDatesToFetch:
    def test_basic_window_ends_yesterday(self):
        days = get_dates_to_fetch(3, today=date(2024, 6, 10))
        assert days == [date(2024, 6, 7), date(2024, 6, 8), date(2024, 6, 9)]

    def test_caps_at_retention(self):
        days = get_dates_to_fetch(100, today=date(2024, 6, 30))
        assert len(days) == ACTIVITY_EVENTS_MAX_RETENTION_DAYS
        assert days[-1] == date(2024, 6, 29)

    def test_minimum_one_day(self):
        days = get_dates_to_fetch(0, today=date(2024, 6, 10))
        assert days == [date(2024, 6, 9)]

    def test_incremental_starts_from_last_loaded(self):
        # last loaded is inside the window -> re-fetch from that day inclusive
        days = get_dates_to_fetch(
            28, today=date(2024, 6, 10), last_loaded_date=date(2024, 6, 7)
        )
        assert days == [date(2024, 6, 7), date(2024, 6, 8), date(2024, 6, 9)]

    def test_incremental_older_than_window_ignored(self):
        # last loaded predates the window -> full window still used
        days = get_dates_to_fetch(
            3, today=date(2024, 6, 10), last_loaded_date=date(2024, 1, 1)
        )
        assert days[0] == date(2024, 6, 7)

    def test_up_to_date_returns_only_yesterday(self):
        # already loaded through yesterday -> just re-fetch yesterday
        days = get_dates_to_fetch(
            28, today=date(2024, 6, 10), last_loaded_date=date(2024, 6, 9)
        )
        assert days == [date(2024, 6, 9)]


class TestDayBounds:
    def test_format(self):
        start, end = day_bounds(date(2024, 6, 1))
        assert start == "2024-06-01T00:00:00"
        assert end == "2024-06-01T23:59:59"
