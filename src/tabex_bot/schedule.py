from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo


TABEX_DAY_TIMES: list[tuple[int, int, list[str]]] = [
    (1, 3, ["08:00", "10:00", "12:00", "14:00", "16:00", "18:00"]),
    (4, 12, ["08:00", "10:30", "13:00", "15:30", "18:00"]),
    (13, 16, ["08:00", "11:00", "14:00", "17:00"]),
    (17, 20, ["08:00", "13:00", "18:00"]),
    (21, 24, ["08:00", "16:00"]),
    (25, 25, ["08:00"]),
]


def _parse_hhmm(value: str) -> time:
    hour_str, minute_str = value.split(":")
    return time(hour=int(hour_str), minute=int(minute_str))


def build_tabex_schedule(start_at_local: datetime, timezone: ZoneInfo) -> list[datetime]:
    """Builds the 25-day Tabex plan anchored to the first pill time."""
    if start_at_local.tzinfo is None:
        start_at_local = start_at_local.replace(tzinfo=timezone)
    else:
        start_at_local = start_at_local.astimezone(timezone)

    baseline = datetime.combine(start_at_local.date(), time(hour=8, minute=0), tzinfo=timezone)
    shift = start_at_local - baseline

    schedule: list[datetime] = []

    for day_start, day_end, times in TABEX_DAY_TIMES:
        for day_number in range(day_start, day_end + 1):
            current_date = start_at_local.date() + timedelta(days=day_number - 1)
            for hhmm in times:
                local_dt = datetime.combine(current_date, _parse_hhmm(hhmm), tzinfo=timezone) + shift
                schedule.append(local_dt)

    schedule.sort()
    return schedule
