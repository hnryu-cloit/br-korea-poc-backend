from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.infrastructure.db.utils import has_table


class StoresRepository:
    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

    async def list_stores(self) -> list[dict]:
        """raw_store_master에서 점포 목록 반환"""
        if not self.engine or not has_table(self.engine, "raw_store_master"):
            return []
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT
                            masked_stor_cd AS store_id,
                            maked_stor_nm  AS store_name,
                            sido,
                            region,
                            store_type
                        FROM raw_store_master
                        ORDER BY masked_stor_cd
                    """)
                ).mappings().all()
            return [dict(r) for r in rows]
        except Exception:
            return []