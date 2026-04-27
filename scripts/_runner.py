"""스크립트 공통 진입점 래퍼.

스크립트는 backend/scripts/ 디렉토리 안에서 직접 실행되므로
같은 디렉토리의 _runner를 다음과 같이 import 한다:

    from _runner import run_main

    if __name__ == "__main__":
        run_main(main)              # def main()이 있는 스크립트

또는 def main()이 없는 톱레벨 실행 스크립트:

    from _runner import run_block

    if __name__ == "__main__":
        with run_block(__file__):
            ...기존 톱레벨 코드...

기능:
- 시작/종료 로그(스크립트명, 인자, 소요시간)
- 예외 발생 시 traceback 출력 후 exit(1)
- stdout 즉시 flush
- DATABASE_URL이 환경에 있다면 마스킹된 형태로 표기
"""
from __future__ import annotations

import contextlib
import logging
import os
import sys
import time
import traceback
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def _safe_db_url() -> str:
    raw = os.environ.get("DATABASE_URL", "")
    if not raw:
        return "(unset)"
    # postgresql+psycopg://user:pw@host:port/db → user:***@host:port/db
    try:
        scheme, rest = raw.split("://", 1)
        if "@" in rest:
            creds, host = rest.split("@", 1)
            if ":" in creds:
                user, _ = creds.split(":", 1)
                return f"{scheme}://{user}:***@{host}"
        return f"{scheme}://{rest}"
    except Exception:
        return "(masked)"


def _setup_logging() -> logging.Logger:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT, stream=sys.stdout)
    return logging.getLogger("script")


def run_main(target: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    logger = _setup_logging()
    script_name = Path(sys.argv[0]).name
    logger.info("▶ START %s argv=%s db=%s", script_name, sys.argv[1:], _safe_db_url())
    started = time.monotonic()
    try:
        target(*args, **kwargs)
    except SystemExit as exc:
        elapsed = time.monotonic() - started
        code = exc.code if isinstance(exc.code, int) else (0 if exc.code is None else 1)
        if code == 0:
            logger.info("■ END   %s exit=0 elapsed=%.1fs", script_name, elapsed)
        else:
            logger.error("✗ FAIL  %s exit=%s elapsed=%.1fs", script_name, code, elapsed)
        sys.stdout.flush()
        raise
    except Exception as exc:
        elapsed = time.monotonic() - started
        logger.error("✗ FAIL  %s elapsed=%.1fs err=%s", script_name, elapsed, exc)
        logger.error("traceback:\n%s", traceback.format_exc())
        sys.stdout.flush()
        sys.exit(1)
    else:
        elapsed = time.monotonic() - started
        logger.info("■ END   %s exit=0 elapsed=%.1fs", script_name, elapsed)
        sys.stdout.flush()


@contextlib.contextmanager
def run_block(script_file: str | None = None) -> Iterator[logging.Logger]:
    logger = _setup_logging()
    script_name = Path(script_file or sys.argv[0]).name
    logger.info("▶ START %s argv=%s db=%s", script_name, sys.argv[1:], _safe_db_url())
    started = time.monotonic()
    try:
        yield logger
    except SystemExit:
        elapsed = time.monotonic() - started
        logger.info("■ END   %s elapsed=%.1fs", script_name, elapsed)
        sys.stdout.flush()
        raise
    except Exception as exc:
        elapsed = time.monotonic() - started
        logger.error("✗ FAIL  %s elapsed=%.1fs err=%s", script_name, elapsed, exc)
        logger.error("traceback:\n%s", traceback.format_exc())
        sys.stdout.flush()
        sys.exit(1)
    else:
        elapsed = time.monotonic() - started
        logger.info("■ END   %s exit=0 elapsed=%.1fs", script_name, elapsed)
        sys.stdout.flush()