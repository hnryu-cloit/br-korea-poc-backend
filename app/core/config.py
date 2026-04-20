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
    SBIZ_API_SNS_ANALYSIS_KEY: str = ""
    SBIZ_API_STARTUP_WEATHER_KEY: str = ""
    SBIZ_API_HOTPLACE_KEY: str = ""
    SBIZ_API_SALES_INDEX_KEY: str = ""
    SBIZ_API_BUSINESS_DURATION_KEY: str = ""
    SBIZ_API_STORE_STATUS_KEY: str = ""
    SBIZ_API_COMMERCIAL_MAP_KEY: str = ""
    SBIZ_API_DETAIL_ANALYSIS_KEY: str = ""
    SBIZ_API_DELIVERY_ANALYSIS_KEY: str = ""
    SBIZ_API_TOUR_FESTIVAL_KEY: str = ""
    SBIZ_API_SIMPLE_ANALYSIS_KEY: str = ""

    AI_SERVICE_URL: str = ""
    AI_SERVICE_TOKEN: str = ""
    MOCK_NOW_STR: str = ""
    ALLOW_CLIENT_ROLE_HEADER_NON_LOCAL: bool = False
    ROLE_OVERRIDE_TOKEN: str = ""

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
        if self.APP_ENV != "local" and self.MOCK_NOW_STR:
            warnings.warn(
                f"운영 환경에서 MOCK_NOW_STR이 설정되어 있습니다: {self.MOCK_NOW_STR}. 실제 시각 대신 고정 시각이 사용됩니다.",
                stacklevel=2,
            )
        if self.APP_ENV != "local" and self.EXTERNAL_API_KEY == "stub-key":
            warnings.warn(
                "운영 환경에서 EXTERNAL_API_KEY가 stub-key 기본값으로 설정되어 있습니다.",
                stacklevel=2,
            )
        if (
            self.APP_ENV != "local"
            and self.ALLOW_CLIENT_ROLE_HEADER_NON_LOCAL
            and not self.ROLE_OVERRIDE_TOKEN
        ):
            warnings.warn(
                "non-local 환경에서 role header 허용 시 ROLE_OVERRIDE_TOKEN 설정을 권장합니다.",
                stacklevel=2,
            )
        return self

    class Config:
        env_file = ".env"


settings = Settings()
