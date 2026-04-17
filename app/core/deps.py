from __future__ import annotations

from datetime import datetime
from typing import Optional

from app.repositories.audit_repository import AuditRepository
from app.repositories.analytics_repository import AnalyticsRepository
from app.core.config import settings
from app.infrastructure.db.connection import get_database_engine, get_safe_database_url
from app.repositories.bootstrap_repository import BootstrapRepository
from app.repositories.data_catalog_repository import DataCatalogRepository
from app.repositories.notifications_repository import NotificationsRepository
from app.repositories.ordering_repository import OrderingRepository
from app.repositories.hq_repository import HQRepository
from app.repositories.production_repository import ProductionRepository
from app.repositories.sales_repository import SalesRepository
from app.repositories.signals_repository import SignalsRepository
from app.services.analytics_service import AnalyticsService
from app.services.ai_client import AIServiceClient
from app.services.audit_service import AuditService
from app.services.bootstrap_service import BootstrapService
from app.services.data_catalog_service import DataCatalogService
from app.services.notifications_service import NotificationsService
from app.services.ordering_service import OrderingService
from app.services.production_service import ProductionService
from app.services.home_service import HomeService
from app.services.hq_service import HQService
from app.services.sales_service import SalesService
from app.services.signals_service import SignalsService


from app.core.utils import get_now


def get_audit_service() -> AuditService:
    return AuditService(repository=AuditRepository(engine=get_database_engine()))


def get_data_catalog_service() -> DataCatalogService:
    return DataCatalogService(
        repository=DataCatalogRepository(engine=get_database_engine()),
        db_path=get_safe_database_url(),
    )


def get_bootstrap_service() -> BootstrapService:
    return BootstrapService(repository=BootstrapRepository(engine=get_database_engine()))


def get_analytics_service() -> AnalyticsService:
    return AnalyticsService(repository=AnalyticsRepository(engine=get_database_engine()))


def get_ordering_service() -> OrderingService:
    return OrderingService(
        repository=OrderingRepository(engine=get_database_engine()),
        audit_service=get_audit_service(),
        ai_client=_get_ai_client(),
    )


def get_production_service() -> ProductionService:
    return ProductionService(
        repository=ProductionRepository(engine=get_database_engine()),
        audit_service=get_audit_service(),
        ai_client=_get_ai_client(),
    )


def get_home_service() -> HomeService:
    return HomeService(
        production_service=get_production_service(),
        ordering_service=get_ordering_service(),
    )


def get_notifications_service() -> NotificationsService:
    return NotificationsService(
        ordering_service=get_ordering_service(),
        production_service=get_production_service(),
        repository=NotificationsRepository(audit_repository=AuditRepository(engine=get_database_engine())),
    )


def _get_ai_client() -> Optional[AIServiceClient]:
    if not settings.AI_SERVICE_URL:
        return None
    return AIServiceClient(base_url=settings.AI_SERVICE_URL, token=settings.AI_SERVICE_TOKEN)


def get_sales_service() -> SalesService:
    return SalesService(
        repository=SalesRepository(engine=get_database_engine()),
        ai_client=_get_ai_client(),
        audit_service=get_audit_service(),
    )


def get_signals_service() -> SignalsService:
    return SignalsService(repository=SignalsRepository(engine=get_database_engine()))


def get_hq_service() -> HQService:
    return HQService(
        repository=HQRepository(engine=get_database_engine()),
        ordering_service=get_ordering_service(),
    )
