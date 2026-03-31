from fastapi import FastAPI

from app.api.routes import router
from app.core.config import settings


app = FastAPI(title="br-korea-poc Backend", version="0.1.0")
app.include_router(router)


@app.get("/")
def root() -> dict[str, str]:
    return {"service": settings.app_name, "status": "running"}
