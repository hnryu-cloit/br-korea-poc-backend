from __future__ import annotations

from collections.abc import Callable
from typing import Optional

from fastapi import Depends, Header, HTTPException, status

UserRole = str

DEFAULT_ROLE = "store_owner"


async def get_current_role(x_user_role: Optional[str] = Header(default=None, alias="X-User-Role")) -> UserRole:
    return x_user_role or DEFAULT_ROLE


def require_roles(*allowed_roles: str) -> Callable[[UserRole], UserRole]:
    async def dependency(role: UserRole = Depends(get_current_role)) -> UserRole:
        if role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "message": "role is not allowed for this resource",
                    "allowed_roles": list(allowed_roles),
                    "current_role": role,
                },
            )
        return role

    return dependency
