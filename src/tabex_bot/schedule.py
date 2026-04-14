from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo


TABEX_DAY_TIMES: list[tuple[int, int, list[str]]] = [
    (1, 3, ["11:00", "13:00", "15:00", "17:00", "19:00", "21:00"]),
    (4, 12, ["11:00", "13:30", "16:00", "18:30", "21:00"]),
    (13, 16, ["11:00", "14:00", "17:00", "20:00"]),
    (17, 20, ["11:00", "16:00", "21:00"]),
    (21, 24, ["11:00", "19:00"]),
    (25, 25, ["11:00"]),
]


def _parse_hhmm(value: str) -> time:
    hour_str, minute_str = value.split(":")
    return time(hour=int(hour_str), minute=int(minute_str))


def build_tabex_schedule(start_at_local: datetime, timezone: ZoneInfo) -> list[datetime]:
    """Builds the 25-day Tabex plan based on calendar dates."""
    if start_at_local.tzinfo is None:
        start_at_local = start_at_local.replace(tzinfo=timezone)
    else:
        start_at_local = start_at_local.astimezone(timezone)

    start_date = start_at_local.date()
    schedule: list[datetime] = []

    for day_start, day_end, times in TABEX_DAY_TIMES:
        for day_number in range(day_start, day_end + 1):
            current_date = start_date + timedelta(days=day_number - 1)
            for hhmm in times:
                local_dt = datetime.combine(current_date, _parse_hhmm(hhmm), tzinfo=timezone)
                schedule.append(local_dt)

    schedule.sort()
    return schedule
