from pydantic import BaseModel


class ChannelDraft(BaseModel):
    format: str
    headline: str
    body: str
    cta: str