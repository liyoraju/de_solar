import os
from datetime import datetime, date
from typing import Optional

BACKFILL_DIR = os.getenv("BACKFILL_DIR", "/app/data/backfill")

TRACKER_FILES = {
    1: os.path.join(BACKFILL_DIR, "last_h1_date.txt"),
    2: os.path.join(BACKFILL_DIR, "last_h2_date.txt"),
    3: os.path.join(BACKFILL_DIR, "last_h3_date.txt"),
    4: os.path.join(BACKFILL_DIR, "last_h4_date.txt"),
}

DEFAULT_DATES = {
    1: os.getenv("DEFAULT_START_DATE_H1", "2026-05-01"),
    2: os.getenv("DEFAULT_START_DATE_H2", "2026-07-01"),
    3: os.getenv("DEFAULT_START_DATE_H3", "2026-06"),
    4: os.getenv("DEFAULT_START_DATE_H4", "2026"),
}

DATE_FORMATS = {
    1: "%Y-%m-%d",
    2: "%Y-%m-%d",
    3: "%Y-%m",
    4: "%Y",
}


def get_last_date(granularity: int) -> Optional[str]:
    path = TRACKER_FILES[granularity]
    fmt = DATE_FORMATS[granularity]
    try:
        with open(path) as f:
            stored = f.read().strip()
            if stored:
                datetime.strptime(stored, fmt)
                return stored
    except (FileNotFoundError, ValueError):
        pass
    return DEFAULT_DATES[granularity]


def save_last_date(granularity: int, date_str: str) -> None:
    path = TRACKER_FILES[granularity]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(date_str)
