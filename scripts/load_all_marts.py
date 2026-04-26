"""모든 사전 계산 마트 일괄 적재 스크립트.

순서가 중요:
1. mart_product_price_*  (제품 판매가)
2. mart_item_category_master  (카테고리 분류 — 다른 마트들이 의존)
3. mart_store_daily_kpi  (mart_item_category_master 의존)
4. mart_hourly_sales_pattern
5. mart_inventory_health_daily
6. mart_campaign_effect_daily
7. mart_payment_mix_daily
"""

from __future__ import annotations

import logging
import sys
from importlib import import_module
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


SCRIPTS = (
    "scripts.load_mart_product_price",
    "scripts.load_mart_item_category",
    "scripts.load_mart_store_daily_kpi",
    "scripts.load_mart_hourly_sales_pattern",
    "scripts.load_mart_inventory_health_daily",
    "scripts.load_mart_campaign_effect_daily",
    "scripts.load_mart_payment_mix_daily",
)


def main() -> None:
    for module_name in SCRIPTS:
        logger.info("===== %s =====", module_name)
        module = import_module(module_name)
        module.main()
    logger.info("모든 마트 적재 완료.")


if __name__ == "__main__":
    main()
