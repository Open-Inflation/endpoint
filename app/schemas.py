from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum


class DynamicsField(str, Enum):
    PRICE = "price"
    DISCOUNT_PRICE = "discount_price"
    LOYAL_PRICE = "loyal_price"
    AVAILABLE_COUNT = "available_count"


class DynamicsInterval(str, Enum):
    ONE_HOUR = "1h"
    SIX_HOURS = "6h"
    ONE_DAY = "1d"
    SEVEN_DAYS = "7d"


INTERVAL_SECONDS: dict[str, int] = {
    DynamicsInterval.ONE_HOUR.value: 3600,
    DynamicsInterval.SIX_HOURS.value: 6 * 3600,
    DynamicsInterval.ONE_DAY.value: 24 * 3600,
    DynamicsInterval.SEVEN_DAYS.value: 7 * 24 * 3600,
}


def parse_iso_datetime_utc(value: str) -> datetime:
    token = str(value).strip()
    if not token:
        raise ValueError("datetime value must be non-empty")
    normalized = token.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("datetime must be valid ISO8601") from exc
    if parsed.tzinfo is None:
        raise ValueError("datetime must include timezone")
    return parsed.astimezone(timezone.utc)


def coerce_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        return parse_iso_datetime_utc(value)
    raise ValueError(f"unsupported datetime value: {value!r}")


def to_iso_z(value: datetime) -> str:
    point = value.astimezone(timezone.utc)
    return point.isoformat().replace("+00:00", "Z")
