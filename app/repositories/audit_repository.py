from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.utils import has_table


class AuditRepository:
    entries: list[dict[str, Any]] = []

    def __init__(self, engine: Engine | None = None) -> None:
        self.engine = engine

    async def add_entry(
        self,
        *,
        domain: str,
        event_type: str,
        actor_role: str,
        route: str,
        outcome: str,
        message: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        if self.engine and has_table(self.engine, "audit_logs"):
            try:
                with self.engine.begin() as connection:
                    row = connection.execute(
                        text(
                            """
                            INSERT INTO audit_logs(
                                domain, event_type, actor_role, route, outcome, message, metadata
                            ) VALUES (
                                :domain, :event_type, :actor_role, :route, :outcome, :message, CAST(:metadata AS JSONB)
                            )
                            RETURNING
                                id,
                                TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') AS timestamp,
                                domain,
                                event_type,
                                actor_role,
                                route,
                                outcome,
                                message,
                                metadata
                            """
                        ),
                        {
                            "domain": domain,
                            "event_type": event_type,
                            "actor_role": actor_role,
                            "route": route,
                            "outcome": outcome,
                            "message": message,
                            "metadata": json.dumps(metadata or {}, ensure_ascii=False),
                        },
                    ).mappings().one()
                    return dict(row)
            except SQLAlchemyError:
                pass

        entry = {
            "id": len(self.entries) + 1,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "domain": domain,
            "event_type": event_type,
            "actor_role": actor_role,
            "route": route,
            "outcome": outcome,
            "message": message,
            "metadata": metadata or {},
        }
        self.entries.append(entry)
        return entry

    async def list_entries(self, domain: Optional[str] = None, limit: int = 50) -> list[dict[str, Any]]:
        if self.engine and has_table(self.engine, "audit_logs"):
            try:
                query = """
                    SELECT
                        id,
                        TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') AS timestamp,
                        domain,
                        event_type,
                        actor_role,
                        route,
                        outcome,
                        message,
                        metadata
                    FROM audit_logs
                """
                params: dict[str, Any] = {"limit": limit}
                if domain:
                    query += " WHERE domain = :domain"
                    params["domain"] = domain
                query += " ORDER BY timestamp DESC LIMIT :limit"
                with self.engine.connect() as connection:
                    rows = connection.execute(text(query), params).mappings().all()
                    return [dict(row) for row in rows]
            except SQLAlchemyError:
                pass

        items = self.entries
        if domain:
            items = [entry for entry in items if entry["domain"] == domain]
        return list(reversed(items))[:limit]
