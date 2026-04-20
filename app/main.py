import logging
from pathlib import Path

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.api.v1.router import api_router
from app.core.config import settings

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

app = FastAPI(
    title="br-korea-poc Backend",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Accept", "X-User-Role"],
)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception(
        "처리되지 않은 예외 발생: method=%s path=%s",
        request.method,
        request.url.path,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "내부 서버 오류가 발생했습니다. 잠시 후 다시 시도해주세요."},
    )


app.include_router(api_router)

menu_image_dir = Path(__file__).resolve().parents[2] / "resource" / "05. 던킨도너츠 메뉴"
if menu_image_dir.exists():
    app.mount("/static/menu-images", StaticFiles(directory=str(menu_image_dir)), name="menu-images")
else:
    logger.warning("메뉴 이미지 디렉터리를 찾을 수 없습니다: %s", menu_image_dir)


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
