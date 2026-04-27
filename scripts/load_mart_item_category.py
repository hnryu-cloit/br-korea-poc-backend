"""mart_item_category_master 적재 스크립트.

품목명을 키워드 룰로 분류한다.
- 도넛/베이글/케이크/머핀/타르트 → category=베이커리, is_food=True
- 커피/아메리카노/라떼/카푸치노/에스프레소 → category=커피, is_coffee=True, is_drink=True
- 음료/주스/에이드/스무디/콜라/티 → category=음료, is_drink=True
- 굿즈/쇼핑백/포장/할인 → category=비제품
- 시즌 키워드(봄/여름/가을/겨울/윈터/설/크리스마스 등) → is_seasonal=True
- [JBOD] 등 접두사 → parent_item_nm 매핑
"""

from __future__ import annotations
from _runner import run_main

import logging
import re
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.infrastructure.db.connection import get_database_engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


COFFEE_KEYWORDS = (
    "아메리카노", "라떼", "카페", "에스프레소", "카푸치노", "콜드브루",
    "마키아또", "모카", "리스트레토", "롱블랙", "플랫화이트",
)
DRINK_KEYWORDS = (
    "주스", "에이드", "스무디", "콜라", "사이다", "스프라이트", "쿨라타",
    "젤리팝", "음료", "차", "티", "탄산", "워터", "쥬스", "프라페", "쉐이크",
    "라우", "쿠라타", "쿨러", "프룻",
)
TEA_KEYWORDS = ("녹차", "홍차", "그린티", "밀크티", "캐모마일", "허브티")
BAKERY_KEYWORDS = (
    "도넛", "도너츠", "도닛", "먼치킨", "베이글", "케이크", "케익", "머핀",
    "타르트", "쿠키", "브라우니", "와플", "스콘", "크로플", "필드", "츄이스티",
    "츄이스틱", "올드훼션드", "글레이즈", "글레이즈드", "보스톤크림",
    "후리터", "롤", "디어", "샌드", "에그랩", "랩", "패스트리", "도", "페이스트리",
    "초코딥", "팝잇", "트위스트", "프레첼", "파이", "번", "휘낭시에", "마들렌",
    "바이트", "바이츠", "소시지", "필딩",
)
SEASON_KEYWORDS = {
    "봄": "spring", "여름": "summer", "가을": "fall", "겨울": "winter",
    "윈터": "winter", "썸머": "summer", "스프링": "spring", "오텀": "fall",
    "할로윈": "halloween", "크리스마스": "christmas", "설": "lunar_new_year",
    "추석": "chuseok", "발렌타인": "valentine",
}
NON_PRODUCT_KEYWORDS = (
    "쇼핑백", "포장", "선물포장", "할인", "공용사용컵", "사용컵", "수수료",
    "환불", "교환", "테스트",
)
RAW_MATERIAL_NAME_KEYWORDS = (
    "\ub0c9\uc7a5",
    "\ub0c9\ub3d9",
    "\ubc18\uc81c",
)
MANUAL_CATEGORY_OVERRIDES = {
    "\ubca0\ub9ac \uc778 \ub7ec\ube0c": "\ubca0\uc774\ucee4\ub9ac",
    "\ube45 \uc560\ud50c\uc2dc\ub098\ubaac": "\ubca0\uc774\ucee4\ub9ac",
    "\uc218\uc90d\uc740 \uc2a4\ub9c8\uc77c": "\ubca0\uc774\ucee4\ub9ac",
    "\uc218\uc90d\uc740\uc2a4\ub9c8\uc77c": "\ubca0\uc774\ucee4\ub9ac",
}
PREFIX_PATTERN = re.compile(r"^\s*\[[^\]]+\]\s*")


def categorize(item_nm: str) -> dict:
    name = item_nm.strip()
    parent = None
    if PREFIX_PATTERN.match(name):
        parent = PREFIX_PATTERN.sub("", name).strip()

    norm = (parent or name).replace(" ", "")
    override_category = MANUAL_CATEGORY_OVERRIDES.get(name) or (
        MANUAL_CATEGORY_OVERRIDES.get(parent) if parent else None
    )
    is_raw_material = any(keyword in name or keyword in norm for keyword in RAW_MATERIAL_NAME_KEYWORDS)

    is_coffee = any(k in norm for k in COFFEE_KEYWORDS)
    is_tea = any(k in norm for k in TEA_KEYWORDS)
    is_drink = (
        is_coffee or is_tea or any(k in norm for k in DRINK_KEYWORDS)
    )
    is_food = any(k in norm for k in BAKERY_KEYWORDS)

    season_tag = None
    is_seasonal = False
    for kw, tag in SEASON_KEYWORDS.items():
        if kw in name or kw in norm:
            season_tag = tag
            is_seasonal = True
            break

    is_non_product = any(k in norm for k in NON_PRODUCT_KEYWORDS)

    if is_non_product:
        category = "비제품"
        is_food = is_drink = is_coffee = False
    elif is_coffee:
        category = "커피"
    elif is_tea:
        category = "차"
    elif is_drink:
        category = "음료"
    elif is_food:
        category = "베이커리"
    else:
        category = "기타"

    matched: list[str] = []
    if is_coffee:
        matched.extend(k for k in COFFEE_KEYWORDS if k in norm)
    if is_tea:
        matched.extend(k for k in TEA_KEYWORDS if k in norm)
    if is_drink and not is_coffee and not is_tea:
        matched.extend(k for k in DRINK_KEYWORDS if k in norm)
    if is_food:
        matched.extend(k for k in BAKERY_KEYWORDS if k in norm)

    return {
        "category": category,
        "is_coffee": bool(is_coffee),
        "is_drink": bool(is_drink),
        "is_food": bool(is_food and not is_non_product),
        "is_seasonal": bool(is_seasonal),
        "season_tag": season_tag,
        "parent_item_nm": parent,
        "keyword_match": ",".join(sorted(set(matched)))[:255] if matched else None,
    }


def fetch_distinct_items(engine) -> list[dict]:
    sql = text(
        """
        SELECT
            COALESCE(NULLIF(TRIM(item_cd), ''), item_nm) AS item_cd,
            item_nm,
            SUM(sale_qty) AS sample_qty
        FROM core_daily_item_sales
        WHERE item_nm IS NOT NULL AND TRIM(item_nm) <> ''
        GROUP BY item_cd, item_nm
        """
    )
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(sql).mappings().all()]


def upsert(engine, rows: list[dict]) -> None:
    if not rows:
        return
    sql = text(
        """
        INSERT INTO mart_item_category_master (
            item_cd, item_nm, category, is_coffee, is_drink, is_food,
            is_seasonal, season_tag, parent_item_nm, keyword_match,
            sample_qty, updated_at
        ) VALUES (
            :item_cd, :item_nm, :category, :is_coffee, :is_drink, :is_food,
            :is_seasonal, :season_tag, :parent_item_nm, :keyword_match,
            :sample_qty, NOW()
        )
        ON CONFLICT (item_cd, item_nm) DO UPDATE SET
            category = EXCLUDED.category,
            is_coffee = EXCLUDED.is_coffee,
            is_drink = EXCLUDED.is_drink,
            is_food = EXCLUDED.is_food,
            is_seasonal = EXCLUDED.is_seasonal,
            season_tag = EXCLUDED.season_tag,
            parent_item_nm = EXCLUDED.parent_item_nm,
            keyword_match = EXCLUDED.keyword_match,
            sample_qty = EXCLUDED.sample_qty,
            updated_at = NOW()
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)


def main() -> None:
    engine = get_database_engine()
    if engine is None:
        raise RuntimeError("Database engine is unavailable.")

    logger.info("품목 목록 조회 중...")
    items = fetch_distinct_items(engine)
    logger.info("distinct items=%d", len(items))

    payload = []
    for row in items:
        info = categorize(str(row["item_nm"]))
        payload.append(
            {
                "item_cd": row["item_cd"],
                "item_nm": row["item_nm"],
                **info,
                "sample_qty": float(row["sample_qty"] or 0),
            }
        )

    counts: dict[str, int] = {}
    for r in payload:
        counts[r["category"]] = counts.get(r["category"], 0) + 1
    logger.info("category distribution: %s", counts)
    coffee_count = sum(1 for r in payload if r["is_coffee"])
    seasonal_count = sum(1 for r in payload if r["is_seasonal"])
    logger.info("is_coffee=%d, is_seasonal=%d", coffee_count, seasonal_count)

    logger.info("mart_item_category_master upsert...")
    upsert(engine, payload)
    logger.info("완료.")


if __name__ == "__main__":
    run_main(main)