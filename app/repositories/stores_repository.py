from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


class StoresRepository:
    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

    async def list_stores(self) -> list[dict]:
        """raw_store_master에서 점포 목록 반환"""
        if not self.engine:
            return []
        try:
            with self.engine.connect() as conn:
                rows = (
                    conn.execute(
                        text(
                            """
                        SELECT
                            masked_stor_cd AS store_id,
                            maked_stor_nm  AS store_name,
                            sido,
                            region,
                            store_type,
                            business_type,
                            store_area_pyeong
                        FROM raw_store_master
                        ORDER BY masked_stor_cd
                    """
                        )
                    )
                    .mappings()
                    .all()
                )
            return [dict(r) for r in rows]
        except SQLAlchemyError:
            return []
