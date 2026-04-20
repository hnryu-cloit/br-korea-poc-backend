import warnings
from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "br-korea-poc"
    APP_ENV: str = "local"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    DATABASE_URL: str = "postgresql+psycopg://postgres:postgres@localhost:5435/br_korea_poc"
    EXTERNAL_API_KEY: str = "stub-key"

    MOCK_NOW_STR: str = "2026-03-10 14:00:00"

    AI_SERVICE_URL: str = ""
    AI_SERVICE_TOKEN: str = ""

    CORS_ORIGINS: str = "http://localhost:5173,http://localhost:6003"

    @property
    def backend_root(self) -> Path:
        return Path(__file__).resolve().parents[2]

    @property
    def project_root(self) -> Path:
        return self.backend_root.parent

    @property
    def resource_root(self) -> Path:
        return (self.project_root / "resource").resolve()

    @property
    def migration_root(self) -> Path:
        return (self.backend_root / "db/migrations").resolve()

    @property
    def manifest_path(self) -> Path:
        return (self.backend_root / "db/manifests/resource_load_manifest.json").resolve()

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",")]

    @model_validator(mode="after")
    def _validate_settings(self) -> "Settings":
        if self.AI_SERVICE_URL and not self.AI_SERVICE_URL.startswith(("http://", "https://")):
            warnings.warn(
                f"AI_SERVICE_URL 형식이 올바르지 않을 수 있습니다: {self.AI_SERVICE_URL}",
                stacklevel=2,
            )
        if self.APP_ENV == "production" and not self.AI_SERVICE_TOKEN:
            warnings.warn(
                "운영 환경에서 AI_SERVICE_TOKEN이 설정되지 않았습니다.",
                stacklevel=2,
            )
        return self

    class Config:
        env_file = ".env"


settings = Settings()
