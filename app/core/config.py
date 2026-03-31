from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "br-korea-poc"
    APP_ENV: str = "local"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    DATABASE_URL: str = "postgresql+psycopg://postgres:postgres@localhost:5435/br_korea_poc"
    EXTERNAL_API_KEY: str = "stub-key"

    AI_SERVICE_URL: str = ""
    AI_SERVICE_TOKEN: str = ""

    CORS_ORIGINS: str = "http://localhost:5173"

    @property
    def backend_root(self) -> Path:
        return Path(__file__).resolve().parents[2]

    @property
    def project_root(self) -> Path:
        return self.backend_root.parent

    @property
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

    class Config:
        env_file = ".env"


settings = Settings()
