from pydantic import BaseModel


class ReviewChecklistItem(BaseModel):
    stage: str
    owner: str
    status: str
    notes: str