from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import inspect, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine, get_safe_database_url
from app.infrastructure.db.utils import has_table

TABLE_OVERRIDES: dict[str, dict[str, Any]] = {
    "raw_store_master": {
        "layer": "raw",
        "domain": "master",
        "description": "점포 기준 정보 원본 적재 테이블",
        "grain": "1 row per store master snapshot row",
        "preferred_for_llm": True,
    },
    "raw_daily_store_item": {
        "layer": "raw",
        "domain": "sales",
        "description": "일자별 상품 매출 원본 적재 테이블",
        "grain": "1 row per store + sale_dt + item_cd",
    },
    "raw_daily_store_item_tmzon": {
        "layer": "raw",
        "domain": "sales",
        "description": "시간대별 상품 매출 원본 적재 테이블",
        "grain": "1 row per store + sale_dt + tmzon_div + item_cd",
    },
    "raw_daily_store_online": {
        "layer": "raw",
        "domain": "sales",
        "description": "채널/온오프라인 매출 원본 적재 테이블",
        "grain": "1 row per store + sale_dt + tmzon_div + channel",
    },
    "raw_daily_store_pay_way": {
        "layer": "raw",
        "domain": "sales",
        "description": "결제수단별 매출 원본 적재 테이블",
        "grain": "1 row per store + sale_dt + pay code",
    },
    "raw_production_extract": {
        "layer": "raw",
        "domain": "production",
        "description": "생산 원장 workbook direct load 테이블",
        "grain": "1 row per store + prod_dt + item_cd + prod_dgre",
    },
    "raw_order_extract": {
        "layer": "raw",
        "domain": "ordering",
        "description": "주문 원장 workbook direct load 테이블",
        "grain": "1 row per store + dlv_dt + item_cd + ord_dgre",
    },
    "raw_inventory_extract": {
        "layer": "raw",
        "domain": "production",
        "description": "재고 원장 workbook direct load 테이블",
        "grain": "1 row per store + stock_dt + item_cd",
    },
    "raw_campaign_master": {
        "layer": "raw",
        "domain": "campaign",
        "description": "캠페인 마스터 원본 적재 테이블",
        "grain": "1 row per CPI_CD",
    },
    "raw_campaign_item_group": {
        "layer": "raw",
        "domain": "campaign",
        "description": "캠페인 상품 그룹 원본 적재 테이블",
        "grain": "1 row per CPI_CD + CPI_ITEM_GRP_CD",
    },
    "raw_campaign_item": {
        "layer": "raw",
        "domain": "campaign",
        "description": "캠페인 대상 품목 원본 적재 테이블",
        "grain": "1 row per CPI_CD + CPI_ITEM_GRP_CD + ITEM_CD",
    },
    "raw_settlement_master": {
        "layer": "raw",
        "domain": "settlement",
        "description": "정산 기준 정보 원본 적재 테이블",
        "grain": "1 row per pay discount type + coop code",
    },
    "raw_telecom_discount_type": {
        "layer": "raw",
        "domain": "discount",
        "description": "통신사 할인 유형 원본 적재 테이블",
        "grain": "1 row per pay group type + pay code + sales org",
    },
    "raw_telecom_discount_policy": {
        "layer": "raw",
        "domain": "discount",
        "description": "통신사 할인 정책 원본 적재 테이블",
        "grain": "1 row per pay group type + pay code + grade + start_dt",
    },
    "raw_telecom_discount_item": {
        "layer": "raw",
        "domain": "discount",
        "description": "통신사 할인 대상 품목 원본 적재 테이블",
        "grain": "1 row per pay group type + pay code + grade + item_cd",
    },
    "core_store_master": {
        "layer": "core",
        "domain": "master",
        "description": "점포 기준 정보 정제 뷰. 점포 필터의 기본 출발점으로 사용",
        "grain": "1 row per store",
        "preferred_for_llm": True,
    },
    "core_daily_item_sales": {
        "layer": "core",
        "domain": "sales",
        "description": "일자별 상품 매출 정제 뷰. 매출 집계/비교 질의에 우선 사용",
        "grain": "1 row per store + sale_dt + item_cd",
        "preferred_for_llm": True,
    },
    "core_hourly_item_sales": {
        "layer": "core",
        "domain": "sales",
        "description": "시간대별 상품 매출 정제 뷰. 피크타임/시간대 분석에 우선 사용",
        "grain": "1 row per store + sale_dt + tmzon_div + item_cd",
        "preferred_for_llm": True,
    },
    "core_channel_sales": {
        "layer": "core",
        "domain": "sales",
        "description": "채널 매출 정제 뷰. 온오프라인/채널 분석에 우선 사용",
        "grain": "1 row per store + sale_dt + tmzon_div + channel",
        "preferred_for_llm": True,
    },
    "audit_logs": {
        "layer": "operational",
        "domain": "audit",
        "description": "질의 처리, 차단, 라우팅 결과에 대한 감사 로그",
        "grain": "1 row per processed event",
        "preferred_for_llm": False,
        "is_sensitive": True,
    },
    "ordering_selections": {
        "layer": "operational",
        "domain": "ordering",
        "description": "점주의 주문안 선택 결과 저장 테이블",
        "grain": "1 row per confirmed selection",
        "preferred_for_llm": True,
    },
    "production_registrations": {
        "layer": "operational",
        "domain": "production",
        "description": "점주의 생산 등록 결과 저장 테이블",
        "grain": "1 row per registration",
        "preferred_for_llm": True,
    },
    "user_bookmarks": {
        "layer": "operational",
        "domain": "bookmark",
        "description": "사용자 저장 관심 항목 북마크 테이블",
        "grain": "1 row per store + type + ref_id",
    },
    "ingestion_runs": {
        "layer": "meta",
        "domain": "ingestion",
        "description": "적재 실행 이력 헤더 테이블",
        "grain": "1 row per ingestion run",
    },
    "ingestion_files": {
        "layer": "meta",
        "domain": "ingestion",
        "description": "파일별 적재 실행 상세 이력 테이블",
        "grain": "1 row per file loaded in a run",
    },
}

COLUMN_DESCRIPTIONS: dict[str, str] = {
    "masked_stor_cd": "매장 코드. 점포 기준 필터의 핵심 키",
    "store_id": "매장 코드. 운영 테이블에서 사용하는 점포 키",
    "masked_stor_nm": "매장명",
    "maked_stor_nm": "매장명 원본 컬럼",
    "item_cd": "상품 코드",
    "item_nm": "상품명",
    "sale_dt": "영업 일자",
    "prod_dt": "생산 일자",
    "dlv_dt": "납품/주문 기준 일자",
    "stock_dt": "재고 기준 일자",
    "tmzon_div": "시간대 구분 값",
    "sale_qty": "판매 수량",
    "sale_amt": "매출 금액",
    "actual_sale_amt": "실매출 금액",
    "net_sale_amt": "순매출 금액",
    "prod_qty": "생산 수량",
    "stock_qty": "재고 수량",
    "ord_qty": "주문 수량",
    "confrm_qty": "확정 주문 수량",
    "option_id": "주문 추천 옵션 식별자",
    "selected_at": "선택 저장 시각",
    "registered_at": "생산 등록 시각",
    "timestamp": "감사 로그 생성 시각",
    "metadata": "추가 메타데이터 JSON",
    "cpi_cd": "캠페인 코드",
    "cpi_item_grp_cd": "캠페인 상품 그룹 코드",
    "pay_dc_cd": "할인/결제 코드",
    "pay_dc_grp_type": "결제/할인 그룹 타입",
}

RELATIONSHIPS: list[dict[str, Any]] = [
    {
        "from_table": "ingestion_files",
        "to_table": "ingestion_runs",
        "relationship_type": "many_to_one",
        "physical_fk": True,
        "join_expression": "ingestion_files.run_id = ingestion_runs.run_id",
        "confidence": "physical",
        "description": "파일 적재 이력은 하나의 적재 실행에 속한다",
        "from_columns": ["run_id"],
        "to_columns": ["run_id"],
    },
    {
        "from_table": "core_daily_item_sales",
        "to_table": "core_store_master",
        "relationship_type": "many_to_one",
        "physical_fk": False,
        "join_expression": (
            "core_daily_item_sales.masked_stor_cd = core_store_master.masked_stor_cd"
        ),
        "confidence": "logical",
        "description": "매출 집계는 점포 마스터와 매장 코드 기준으로 연결한다",
        "from_columns": ["masked_stor_cd"],
        "to_columns": ["masked_stor_cd"],
    },
    {
        "from_table": "core_hourly_item_sales",
        "to_table": "core_store_master",
        "relationship_type": "many_to_one",
        "physical_fk": False,
        "join_expression": (
            "core_hourly_item_sales.masked_stor_cd = core_store_master.masked_stor_cd"
        ),
        "confidence": "logical",
        "description": "시간대 매출은 점포 마스터와 매장 코드 기준으로 연결한다",
        "from_columns": ["masked_stor_cd"],
        "to_columns": ["masked_stor_cd"],
    },
    {
        "from_table": "core_channel_sales",
        "to_table": "core_store_master",
        "relationship_type": "many_to_one",
        "physical_fk": False,
        "join_expression": (
            "core_channel_sales.masked_stor_cd = core_store_master.masked_stor_cd"
        ),
        "confidence": "logical",
        "description": "채널 매출은 점포 마스터와 매장 코드 기준으로 연결한다",
        "from_columns": ["masked_stor_cd"],
        "to_columns": ["masked_stor_cd"],
    },
    {
        "from_table": "ordering_selections",
        "to_table": "raw_store_master",
        "relationship_type": "many_to_one",
        "physical_fk": False,
        "join_expression": "ordering_selections.store_id = raw_store_master.masked_stor_cd",
        "confidence": "logical",
        "description": "주문 선택 결과는 점포 마스터와 논리적으로 매핑된다",
        "from_columns": ["store_id"],
        "to_columns": ["masked_stor_cd"],
    },
    {
        "from_table": "production_registrations",
        "to_table": "raw_store_master",
        "relationship_type": "many_to_one",
        "physical_fk": False,
        "join_expression": (
            "production_registrations.store_id = raw_store_master.masked_stor_cd"
        ),
        "confidence": "logical",
        "description": "생산 등록 결과는 점포 마스터와 논리적으로 매핑된다",
        "from_columns": ["store_id"],
        "to_columns": ["masked_stor_cd"],
    },
    {
        "from_table": "raw_campaign_item_group",
        "to_table": "raw_campaign_master",
        "relationship_type": "many_to_one",
        "physical_fk": False,
        "join_expression": "raw_campaign_item_group.CPI_CD = raw_campaign_master.CPI_CD",
        "confidence": "logical",
        "description": "캠페인 상품 그룹은 캠페인 마스터에 속한다",
        "from_columns": ["CPI_CD"],
        "to_columns": ["CPI_CD"],
    },
    {
        "from_table": "raw_campaign_item",
        "to_table": "raw_campaign_item_group",
        "relationship_type": "many_to_one",
        "physical_fk": False,
        "join_expression": (
            "raw_campaign_item.CPI_CD = raw_campaign_item_group.CPI_CD AND "
            "raw_campaign_item.CPI_ITEM_GRP_CD = raw_campaign_item_group.CPI_ITEM_GRP_CD"
        ),
        "confidence": "logical",
        "description": "캠페인 품목은 캠페인 상품 그룹에 속한다",
        "from_columns": ["CPI_CD", "CPI_ITEM_GRP_CD"],
        "to_columns": ["CPI_CD", "CPI_ITEM_GRP_CD"],
    },
    {
        "from_table": "raw_telecom_discount_policy",
        "to_table": "raw_telecom_discount_type",
        "relationship_type": "many_to_one",
        "physical_fk": False,
        "join_expression": (
            "raw_telecom_discount_policy.PAY_DC_GRP_TYPE = "
            "raw_telecom_discount_type.PAY_DC_GRP_TYPE AND "
            "raw_telecom_discount_policy.PAY_DC_CD = raw_telecom_discount_type.PAY_DC_CD"
        ),
        "confidence": "logical",
        "description": "통신사 할인 정책은 할인 유형에 속한다",
        "from_columns": ["PAY_DC_GRP_TYPE", "PAY_DC_CD"],
        "to_columns": ["PAY_DC_GRP_TYPE", "PAY_DC_CD"],
    },
    {
        "from_table": "raw_telecom_discount_item",
        "to_table": "raw_telecom_discount_policy",
        "relationship_type": "many_to_one",
        "physical_fk": False,
        "join_expression": (
            "raw_telecom_discount_item.PAY_DC_GRP_TYPE = "
            "raw_telecom_discount_policy.PAY_DC_GRP_TYPE AND "
            "raw_telecom_discount_item.PAY_DC_CD = raw_telecom_discount_policy.PAY_DC_CD AND "
            "raw_telecom_discount_item.COOP_CMP_GRADE_CD = "
            "raw_telecom_discount_policy.COOP_CMP_GRADE_CD"
        ),
        "confidence": "logical",
        "description": "통신사 할인 대상 품목은 할인 정책에 속한다",
        "from_columns": ["PAY_DC_GRP_TYPE", "PAY_DC_CD", "COOP_CMP_GRADE_CD"],
        "to_columns": ["PAY_DC_GRP_TYPE", "PAY_DC_CD", "COOP_CMP_GRADE_CD"],
    },
]

TABLE_EXAMPLES: dict[str, list[dict[str, str]]] = {
    "core_daily_item_sales": [
        {
            "use_case": "sales_compare",
            "question": "이번 달 우리 매장 상품별 매출 상위 10개를 보여줘",
            "sql_template": (
                "SELECT item_cd, item_nm, SUM(sale_amt) AS total_sale_amt "
                "FROM core_daily_item_sales "
                "WHERE masked_stor_cd = :store_id "
                "GROUP BY item_cd, item_nm "
                "ORDER BY total_sale_amt DESC "
                "LIMIT 10"
            ),
            "notes": "점포 필터는 masked_stor_cd 기준으로 먼저 적용한다",
        }
    ],
    "core_hourly_item_sales": [
        {
            "use_case": "peak_hour",
            "question": "우리 매장 피크 타임을 상품 판매량 기준으로 분석해줘",
            "sql_template": (
                "SELECT tmzon_div, SUM(sale_qty) AS total_sale_qty "
                "FROM core_hourly_item_sales "
                "WHERE masked_stor_cd = :store_id "
                "GROUP BY tmzon_div "
                "ORDER BY tmzon_div"
            ),
            "notes": "tmzon_div는 정규화된 시간대 값이다",
        }
    ],
    "core_channel_sales": [
        {
            "use_case": "channel_mix",
            "question": "배달/오프라인 채널별 매출 비중을 보고 싶어",
            "sql_template": (
                "SELECT ho_chnl_div, SUM(sale_amt) AS total_sale_amt "
                "FROM core_channel_sales "
                "WHERE masked_stor_cd = :store_id "
                "GROUP BY ho_chnl_div"
            ),
            "notes": "채널 분석은 core_channel_sales 우선 사용",
        }
    ],
    "ordering_selections": [
        {
            "use_case": "ordering_history",
            "question": "최근 주문 추천안 선택 이력을 보여줘",
            "sql_template": (
                "SELECT option_id, actor, selected_at "
                "FROM ordering_selections "
                "WHERE store_id = :store_id "
                "ORDER BY selected_at DESC "
                "LIMIT 20"
            ),
            "notes": "운영 테이블은 실제 사용자 행동 데이터다",
        }
    ],
    "production_registrations": [
        {
            "use_case": "production_history",
            "question": "최근 생산 등록 이력을 보여줘",
            "sql_template": (
                "SELECT sku_id, qty, registered_at "
                "FROM production_registrations "
                "WHERE store_id = :store_id "
                "ORDER BY registered_at DESC "
                "LIMIT 20"
            ),
            "notes": "SKU 단위 운영 이력 추적에 사용",
        }
    ],
}


def infer_layer(table_name: str, is_view: bool) -> str:
    if table_name in {"schema_migrations", "ingestion_runs", "ingestion_files"}:
        return "meta"
    if is_view or table_name.startswith("core_"):
        return "core"
    if table_name in {"audit_logs", "ordering_selections", "production_registrations", "user_bookmarks"}:
        return "operational"
    return "raw"


def infer_domain(table_name: str) -> str:
    lowered = table_name.lower()
    if "production" in lowered or "inventory" in lowered:
        return "production"
    if "order" in lowered:
        return "ordering"
    if "campaign" in lowered:
        return "campaign"
    if "telecom" in lowered or "settlement" in lowered or "pay" in lowered:
        return "discount"
    if "audit" in lowered:
        return "audit"
    if "bookmark" in lowered:
        return "bookmark"
    if lowered.startswith("core_") or "sale" in lowered or "channel" in lowered:
        return "sales"
    if "store" in lowered:
        return "master"
    return "general"


def infer_description(table_name: str) -> str:
    override = TABLE_OVERRIDES.get(table_name)
    if override and override.get("description"):
        return str(override["description"])
    if table_name.startswith("raw_"):
        return f"{table_name} 원본 적재 테이블"
    if table_name.startswith("core_"):
        return f"{table_name} 앱 조회용 정제 뷰"
    return f"{table_name} 메타데이터 객체"


def infer_grain(table_name: str) -> str | None:
    override = TABLE_OVERRIDES.get(table_name)
    if override and "grain" in override:
        return override["grain"]
    return None


def infer_object_type(table_name: str, view_names: set[str]) -> str:
    return "view" if table_name in view_names or table_name.startswith("core_") else "table"


def infer_preferred_for_llm(table_name: str) -> bool:
    override = TABLE_OVERRIDES.get(table_name)
    if override and "preferred_for_llm" in override:
        return bool(override["preferred_for_llm"])
    return table_name.startswith("core_")


def infer_sensitive(table_name: str) -> bool:
    override = TABLE_OVERRIDES.get(table_name)
    if override and "is_sensitive" in override:
        return bool(override["is_sensitive"])
    return table_name in {"audit_logs"}


def infer_semantic_role(column_name: str) -> str | None:
    lowered = column_name.lower()
    if lowered in {"masked_stor_cd", "store_id"}:
        return "store_key"
    if lowered == "item_cd":
        return "item_key"
    if lowered.endswith("_dt") or lowered in {"sale_dt", "prod_dt", "dlv_dt", "stock_dt"}:
        return "business_date"
    if lowered in {"created_at", "updated_at", "selected_at", "registered_at", "timestamp"}:
        return "event_time"
    if lowered.endswith("_amt"):
        return "amount_measure"
    if lowered.endswith("_qty") or lowered.endswith("_cnt") or lowered.endswith("_rate"):
        return "numeric_measure"
    if lowered.endswith("_nm") or lowered.endswith("_name"):
        return "label"
    if lowered == "metadata":
        return "json_metadata"
    return None


def is_filter_key(column_name: str) -> bool:
    lowered = column_name.lower()
    return lowered in {
        "masked_stor_cd",
        "store_id",
        "item_cd",
        "sale_dt",
        "prod_dt",
        "dlv_dt",
        "stock_dt",
        "cpi_cd",
        "pay_dc_cd",
        "pay_dc_grp_type",
    }


def is_time_key(column_name: str) -> bool:
    lowered = column_name.lower()
    return lowered.endswith("_dt") or lowered in {
        "created_at",
        "updated_at",
        "selected_at",
        "registered_at",
        "timestamp",
        "loaded_at",
    }


def is_measure(column_name: str) -> bool:
    lowered = column_name.lower()
    return lowered.endswith(("_amt", "_qty", "_cnt", "_rate", "_pct"))


def column_description(column_name: str) -> str:
    return COLUMN_DESCRIPTIONS.get(column_name.lower(), "")


def default_example_values(column_name: str) -> list[str]:
    lowered = column_name.lower()
    if lowered in {"masked_stor_cd", "store_id"}:
        return ["POC_001"]
    if lowered == "item_cd":
        return ["811047"]
    if lowered in {"sale_dt", "prod_dt", "dlv_dt", "stock_dt"}:
        return ["2026-03-10"]
    if lowered == "tmzon_div":
        return ["09", "14", "18"]
    return []


def table_metadata_payload(table_name: str, object_type: str) -> dict[str, Any]:
    override = TABLE_OVERRIDES.get(table_name, {})
    return {
        "table_name": table_name,
        "layer": override.get("layer", infer_layer(table_name, object_type == "view")),
        "object_type": object_type,
        "domain": override.get("domain", infer_domain(table_name)),
        "description": infer_description(table_name),
        "grain": infer_grain(table_name),
        "preferred_for_llm": infer_preferred_for_llm(table_name),
        "is_sensitive": infer_sensitive(table_name),
        "source_of_truth": "seed",
    }


def main() -> None:
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError(
            "PostgreSQL driver is not installed. Install psycopg before seeding schema catalog."
        )
    if not has_table(engine, "schema_catalog_tables"):
        raise RuntimeError(
            "schema_catalog tables do not exist. Run scripts/migrate_db.py first."
        )

    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    view_names = set(inspector.get_view_names())
    excluded = {
        "schema_migrations",
        "schema_catalog_tables",
        "schema_catalog_columns",
        "schema_catalog_relationships",
        "schema_catalog_examples",
    }
    object_names = sorted((table_names | view_names) - excluded)

    with engine.begin() as connection:
        connection.execute(text("DELETE FROM schema_catalog_examples"))
        connection.execute(text("DELETE FROM schema_catalog_relationships"))
        connection.execute(text("DELETE FROM schema_catalog_columns"))
        connection.execute(text("DELETE FROM schema_catalog_tables"))

        for table_name in object_names:
            object_type = infer_object_type(table_name, view_names)
            connection.execute(
                text(
                    """
                    INSERT INTO schema_catalog_tables(
                        table_name,
                        layer,
                        object_type,
                        domain,
                        description,
                        grain,
                        preferred_for_llm,
                        is_sensitive,
                        source_of_truth
                    ) VALUES (
                        :table_name,
                        :layer,
                        :object_type,
                        :domain,
                        :description,
                        :grain,
                        :preferred_for_llm,
                        :is_sensitive,
                        :source_of_truth
                    )
                    """
                ),
                table_metadata_payload(table_name, object_type),
            )

            try:
                columns = inspector.get_columns(table_name)
            except Exception:
                columns = []
            try:
                pk_constraint = inspector.get_pk_constraint(table_name) or {}
                pk_columns = set(pk_constraint.get("constrained_columns") or [])
            except Exception:
                pk_columns = set()

            for ordinal_position, column in enumerate(columns, start=1):
                column_name = column["name"]
                data_type = str(column.get("type") or "unknown")
                connection.execute(
                    text(
                        """
                        INSERT INTO schema_catalog_columns(
                            table_name,
                            column_name,
                            data_type,
                            ordinal_position,
                            description,
                            semantic_role,
                            is_primary_key,
                            is_filter_key,
                            is_time_key,
                            is_measure,
                            is_sensitive,
                            example_values_json
                        ) VALUES (
                            :table_name,
                            :column_name,
                            :data_type,
                            :ordinal_position,
                            :description,
                            :semantic_role,
                            :is_primary_key,
                            :is_filter_key,
                            :is_time_key,
                            :is_measure,
                            :is_sensitive,
                            CAST(:example_values_json AS JSONB)
                        )
                        """
                    ),
                    {
                        "table_name": table_name,
                        "column_name": column_name,
                        "data_type": data_type,
                        "ordinal_position": ordinal_position,
                        "description": column_description(column_name),
                        "semantic_role": infer_semantic_role(column_name),
                        "is_primary_key": bool(column_name in pk_columns),
                        "is_filter_key": is_filter_key(column_name),
                        "is_time_key": is_time_key(column_name),
                        "is_measure": is_measure(column_name),
                        "is_sensitive": bool(
                            table_name == "audit_logs" and column_name.lower() == "metadata"
                        ),
                        "example_values_json": json.dumps(default_example_values(column_name)),
                    },
                )

        for relationship in RELATIONSHIPS:
            if relationship["from_table"] not in object_names:
                continue
            if relationship["to_table"] not in object_names:
                continue
            connection.execute(
                text(
                    """
                    INSERT INTO schema_catalog_relationships(
                        from_table,
                        to_table,
                        relationship_type,
                        physical_fk,
                        join_expression,
                        confidence,
                        description,
                        from_columns_json,
                        to_columns_json
                    ) VALUES (
                        :from_table,
                        :to_table,
                        :relationship_type,
                        :physical_fk,
                        :join_expression,
                        :confidence,
                        :description,
                        CAST(:from_columns_json AS JSONB),
                        CAST(:to_columns_json AS JSONB)
                    )
                    """
                ),
                {
                    **relationship,
                    "from_columns_json": json.dumps(relationship["from_columns"]),
                    "to_columns_json": json.dumps(relationship["to_columns"]),
                },
            )

        for table_name, examples in TABLE_EXAMPLES.items():
            if table_name not in object_names:
                continue
            for example in examples:
                connection.execute(
                    text(
                        """
                        INSERT INTO schema_catalog_examples(
                            table_name,
                            use_case,
                            question,
                            sql_template,
                            notes
                        ) VALUES (
                            :table_name,
                            :use_case,
                            :question,
                            :sql_template,
                            :notes
                        )
                        """
                    ),
                    {
                        "table_name": table_name,
                        "use_case": example["use_case"],
                        "question": example["question"],
                        "sql_template": example.get("sql_template"),
                        "notes": example.get("notes"),
                    },
                )

    print(f"Schema catalog seeded to {get_safe_database_url()}")
    print(f"Objects seeded: {len(object_names)}")


if __name__ == "__main__":
    main()
