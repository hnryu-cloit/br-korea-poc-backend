from pydantic import BaseModel


class Settings(BaseModel):
    app_name: str = "br-korea-poc"
    app_env: str = "local"
    database_url: str = "sqlite:///./app.db"
    external_api_key: str = "stub-key"


settings = Settings()
