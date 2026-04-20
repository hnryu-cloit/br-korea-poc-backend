from __future__ import annotations

from app.core.config import settings
from app.infrastructure.db.connection import get_database_engine, get_safe_database_url
from app.repositories.analytics_repository import AnalyticsRepository
from app.repositories.audit_repository import AuditRepository
from app.repositories.bootstrap_repository import BootstrapRepository
from app.repositories.data_catalog_repository import DataCatalogRepository
from app.repositories.home_repository import HomeRepository
from app.repositories.hq_repository import HQRepository
from app.repositories.notifications_repository import NotificationsRepository
from app.repositories.ordering_repository import OrderingRepository
from app.repositories.production_repository import ProductionRepository
from app.repositories.prompt_settings_repository import PromptSettingsRepository
from app.repositories.sales_repository import SalesRepository
from app.repositories.schema_catalog_repository import SchemaCatalogRepository
from app.repositories.signals_repository import SignalsRepository
from app.repositories.stores_repository import StoresRepository
from app.services.ai_client import AIServiceClient
from app.services.analytics_service import AnalyticsService
from app.services.audit_service import AuditService
from app.services.bootstrap_service import BootstrapService
from app.services.data_catalog_service import DataCatalogService
from app.services.home_service import HomeService
from app.services.hq_service import HQService
from app.services.notifications_service import NotificationsService
from app.services.ordering_service import OrderingService
from app.services.production_service import ProductionService
from app.services.prompt_settings_service import PromptSettingsService
from app.services.sales_service import SalesService
from app.services.schema_catalog_service import SchemaCatalogService
from app.services.signals_service import SignalsService
from app.services.stores_service import StoresService


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
        repository=HomeRepository(engine=get_database_engine()),
        prompt_settings_service=get_prompt_settings_service(),
    )


def get_notifications_service() -> NotificationsService:
    return NotificationsService(
        ordering_service=get_ordering_service(),
        production_service=get_production_service(),
        repository=NotificationsRepository(
            audit_repository=AuditRepository(engine=get_database_engine())
        ),
    )


def _get_ai_client() -> AIServiceClient | None:
    if not settings.AI_SERVICE_URL:
        return None
    return AIServiceClient(base_url=settings.AI_SERVICE_URL, token=settings.AI_SERVICE_TOKEN)


def get_sales_service() -> SalesService:
    return SalesService(
        repository=SalesRepository(engine=get_database_engine()),
        ai_client=_get_ai_client(),
        audit_service=get_audit_service(),
        prompt_settings_service=get_prompt_settings_service(),
    )


def get_signals_service() -> SignalsService:
    return SignalsService(repository=SignalsRepository(engine=get_database_engine()))


def get_hq_service() -> HQService:
    return HQService(
        repository=HQRepository(engine=get_database_engine()),
        ordering_service=get_ordering_service(),
    )


def get_stores_service() -> StoresService:
    return StoresService(repository=StoresRepository(engine=get_database_engine()))


def get_prompt_settings_service() -> PromptSettingsService:
    return PromptSettingsService(
        repository=PromptSettingsRepository(
            file_path=settings.backend_root / "data/prompt_settings.json"
        ),
    )


def get_schema_catalog_service() -> SchemaCatalogService:
    return SchemaCatalogService(
        repository=SchemaCatalogRepository(engine=get_database_engine())
    )
