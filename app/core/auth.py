from __future__ import annotations

from collections.abc import Callable

from fastapi import Depends, Header, HTTPException, status

from app.core.config import settings

UserRole = str

DEFAULT_ROLE = "store_owner"
ALLOWED_ROLES = {
    "store_owner",
    "hq_admin",
    "hq_operator",
    "hq_planner",
}


async def get_current_role(
    x_user_role: str | None = Header(default=None, alias="X-User-Role"),
    x_role_token: str | None = Header(default=None, alias="X-Role-Token"),
) -> UserRole:
    role = (x_user_role or DEFAULT_ROLE).strip()
    if role not in ALLOWED_ROLES:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "message": "invalid role",
                "allowed_roles": sorted(ALLOWED_ROLES),
                "current_role": role,
            },
        )

    # local 환경 또는 명시 허용 설정에서는 기존 role header 동작 유지
    if settings.APP_ENV == "local" or settings.ALLOW_CLIENT_ROLE_HEADER_NON_LOCAL:
        if settings.APP_ENV != "local" and settings.ROLE_OVERRIDE_TOKEN:
            if x_role_token != settings.ROLE_OVERRIDE_TOKEN:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={
                        "message": "role override token is required",
                        "current_role": role,
                    },
                )
        return role

    # non-local 기본 정책: client role header를 신뢰하지 않음
    # 단, 운영 점검/관리 목적으로 override token이 일치하면 허용
    if role != DEFAULT_ROLE:
        if settings.ROLE_OVERRIDE_TOKEN and x_role_token == settings.ROLE_OVERRIDE_TOKEN:
            return role
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "message": "role header override is disabled in non-local environments",
                "current_role": role,
            },
        )
    return DEFAULT_ROLE


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
