from fastapi import APIRouter

from app.api.v1.endpoints import (
    analytics,
    audit,
    bootstrap,
    channels,
    data_catalog,
    health,
    hq,
    notifications,
    ordering,
    production,
    review,
    sales,
    signals,
    simulation,
)

api_router = APIRouter(prefix="/api")

api_router.include_router(health.router)
api_router.include_router(audit.router)
api_router.include_router(bootstrap.router)
api_router.include_router(data_catalog.router)
api_router.include_router(simulation.router)
api_router.include_router(channels.router)
api_router.include_router(review.router)
api_router.include_router(ordering.router)
api_router.include_router(production.router)
api_router.include_router(sales.router)
api_router.include_router(notifications.router)
api_router.include_router(analytics.router)
api_router.include_router(hq.router)
api_router.include_router(signals.router)
