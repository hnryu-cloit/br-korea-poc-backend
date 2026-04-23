from __future__ import annotations

from datetime import datetime, timedelta, timezone

_KST = timezone(timedelta(hours=9))


def parse_reference_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = None

    if parsed is None:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(normalized, fmt)
                break
            except ValueError:
                continue
    if parsed is None:
        return None

    if parsed.tzinfo is not None:
        return parsed.astimezone(_KST).replace(tzinfo=None)
    return parsed


def resolve_reference_date(value: str | None) -> str | None:
    parsed = parse_reference_datetime(value)
    if parsed is None:
        return None
    return parsed.strftime("%Y-%m-%d")


def resolve_date_range_by_reference(
    reference_datetime: str | None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> tuple[str | None, str | None]:
    reference_date = resolve_reference_date(reference_datetime)
    return date_from or reference_date, date_to or reference_date
