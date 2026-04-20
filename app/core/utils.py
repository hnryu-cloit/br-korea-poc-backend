from datetime import datetime

from app.core.config import settings


def get_now() -> datetime:
    if settings.MOCK_NOW_STR:
        try:
            return datetime.strptime(settings.MOCK_NOW_STR, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return datetime.now()
    return datetime.now()
