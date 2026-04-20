from pydantic import BaseModel


class BootstrapResponse(BaseModel):
    product: str
    summary: str
    users: list[str]
    goals: list[str]
    policies: list[str]
    features: dict[str, list[dict[str, str]]]
