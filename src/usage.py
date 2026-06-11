"""
Helpers for the Power BI usage (Activity Events) pipeline.

The Activity Events API returns one UTC day per call and retains only ~28 days
of history, so the pipeline iterates day-by-day over a bounded, incremental
window. These pure helpers compute that window and format the per-day request
bounds, kept separate from Spark/HTTP for easy unit testing.
"""

from datetime import date, datetime, timedelta, timezone

# Power BI keeps activity events for roughly 28-30 days; cap requests at the
# conservative end so we never ask for a window the API will reject.
ACTIVITY_EVENTS_MAX_RETENTION_DAYS = 28


def get_dates_to_fetch(
    lookback_days: int,
    today: date | None = None,
    last_loaded_date: date | None = None,
) -> list[date]:
    """Return the ascending list of UTC dates to query activity events for.

    - The window ends at *yesterday* (the most recent complete UTC day).
    - It starts ``lookback_days`` before today, clamped to the API's retention.
    - When ``last_loaded_date`` is provided and falls inside the window, the
      start moves up to that date (inclusive) so the boundary day is re-fetched
      to capture any late-arriving events — the MERGE on ``event_id`` makes the
      overlap idempotent.

    Parameters
    ----------
    lookback_days : int
        How many days back to consider (clamped to 1..28).
    today : datetime.date | None
        Override "today" (UTC). Defaults to the current UTC date.
    last_loaded_date : datetime.date | None
        Latest ``creation_date`` already stored, if any.

    Returns
    -------
    list[datetime.date]
        Dates to fetch, oldest first. Empty if there is nothing to do.
    """
    if today is None:
        today = datetime.now(timezone.utc).date()

    lookback_days = max(1, min(lookback_days, ACTIVITY_EVENTS_MAX_RETENTION_DAYS))

    window_start = today - timedelta(days=lookback_days)
    window_end = today - timedelta(days=1)

    if last_loaded_date is not None and last_loaded_date >= window_start:
        window_start = last_loaded_date

    days: list[date] = []
    current = window_start
    while current <= window_end:
        days.append(current)
        current += timedelta(days=1)
    return days


def day_bounds(day: date) -> tuple[str, str]:
    """Return ``(start, end)`` ISO-8601 UTC strings spanning the full ``day``.

    Format matches the Activity Events API requirement: no timezone offset and
    both timestamps within the same UTC day.
    """
    iso = day.isoformat()
    return f"{iso}T00:00:00", f"{iso}T23:59:59"
