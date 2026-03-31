from app.repositories.bootstrap_repository import BootstrapRepository
from app.repositories.ordering_repository import OrderingRepository
from app.repositories.production_repository import ProductionRepository
from app.repositories.sales_repository import SalesRepository
from app.services.bootstrap_service import BootstrapService
from app.services.ordering_service import OrderingService
from app.services.production_service import ProductionService
from app.services.sales_service import SalesService


def get_bootstrap_service() -> BootstrapService:
    return BootstrapService(repository=BootstrapRepository())


def get_ordering_service() -> OrderingService:
    return OrderingService(repository=OrderingRepository())


def get_production_service() -> ProductionService:
    return ProductionService(repository=ProductionRepository())


def get_sales_service() -> SalesService:
    return SalesService(repository=SalesRepository())
