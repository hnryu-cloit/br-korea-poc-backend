from fastapi import APIRouter

from app.api.v1.endpoints import bootstrap, channels, health, review, simulation

api_router = APIRouter(prefix="/api")

api_router.include_router(health.router)
api_router.include_router(bootstrap.router)
api_router.include_router(simulation.router)
api_router.include_router(channels.router)
api_router.include_router(review.router)