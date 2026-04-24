from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import HTTPException


@asynccontextmanager
async def service_error_handler(domain_label: str) -> AsyncGenerator[None, None]:
    """서비스 계층 예외를 HTTP 상태코드로 변환
    ValueError → 422, LookupError → 404, RuntimeError → 500
    """
    try:
        yield
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=f"{domain_label} 오류: {str(exc)}") from exc