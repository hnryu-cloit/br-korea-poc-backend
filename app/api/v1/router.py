from fastapi import APIRouter

from app.api.v1.endpoints import (
    bootstrap,
    channels,
    health,
    ordering,
    production,
    review,
    sales,
    simulation,
)

api_router = APIRouter(prefix="/api")

api_router.include_router(health.router)
api_router.include_router(bootstrap.router)
api_router.include_router(simulation.router)
api_router.include_router(channels.router)
api_router.include_router(review.router)
api_router.include_router(ordering.router)
api_router.include_router(production.router)
api_router.include_router(sales.router)
