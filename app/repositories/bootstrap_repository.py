from __future__ import annotations


class BootstrapRepository:
    def __init__(self, engine: object | None = None) -> None:
        self.engine = engine

    async def get_bootstrap(self) -> dict:
        return {
            "product": "",
            "summary": "",
            "users": [],
            "goals": [],
            "policies": [],
            "features": {},
        }
