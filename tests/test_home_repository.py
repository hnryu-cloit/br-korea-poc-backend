from __future__ import annotations

from pathlib import Path

from app.repositories.home_repository import HomeRepository


def test_build_dashboard_recommended_questions_prefers_curated_top3(monkeypatch) -> None:
    csv_path = Path(__file__).resolve().parent / "_dashboard_questions_test.csv"
    try:
        csv_path.write_text(
            "\n".join(
                [
                    "질문번호,기준일시,에이전트,질문,평가항목,가용여부,가용 데이터,테이블/컬럼,가정/갭,실제 쿼리,예상 답변",
                    "001,2026-03-05 09:00 (KST),생산 관리,오늘 품절 발생 시각이 가장 빠른 품목 알려줘,항목,⚠️,-,-,-,SELECT 1,답변",
                    "002,2026-03-05 09:00 (KST),생산 관리,오늘 재고 부족(음수 재고) 품목 상위 10개 알려줘,항목,✅,-,-,-,SELECT 1,답변",
                    "003,2026-03-05 09:00 (KST),생산 관리,오늘 시간대별 판매 속도 대비 현재 재고 부족 위험 품목,항목,⚠️,-,-,-,SELECT 1,답변",
                    "004,2026-03-05 09:00 (KST),생산 관리,오늘 과잉 재고 품목 상위 10개 알려줘,항목,✅,-,-,-,SELECT 1,답변",
                    "005,2026-03-05 09:00 (KST),생산 관리,오늘 재고율 하위 10개 품목과 판매량 같이 보여줘,항목,✅,-,-,-,SELECT 1,답변",
                    "006,2026-03-05 09:00 (KST),주문 관리,오늘 자동발주 비율 알려줘,항목,✅,-,-,-,SELECT 1,답변",
                    "007,2026-03-05 09:00 (KST),주문 관리,오늘 발주수량 대비 확정수량 차이가 큰 품목,항목,✅,-,-,-,SELECT 1,답변",
                    "008,2026-03-05 09:00 (KST),주문 관리,오늘 납품예정일 기준 확정 발주 수량 상위 품목,항목,✅,-,-,-,SELECT 1,답변",
                    "009,2026-03-05 09:00 (KST),매출 관리,오늘 시간대별 매출 피크 알려줘,항목,✅,-,-,-,SELECT 1,답변",
                    "010,2026-03-05 09:00 (KST),매출 관리,오늘 총매출과 일평균 매출 알려줘,항목,✅,-,-,-,SELECT 1,답변",
                    "011,2026-03-05 09:00 (KST),매출 관리,오늘 상품 매출 상위 10개 알려줘,항목,✅,-,-,-,SELECT 1,답변",
                ]
            ),
            encoding="utf-8-sig",
        )

        monkeypatch.setattr(
            HomeRepository,
            "_golden_queries_csv_path",
            staticmethod(lambda: csv_path),
        )

        result = HomeRepository._build_dashboard_recommended_questions()

        assert [entry["question"] for entry in result["production"]] == [
            "오늘 재고 부족(음수 재고) 품목 상위 10개 알려줘",
            "오늘 과잉 재고 품목 상위 10개 알려줘",
            "오늘 재고율 하위 10개 품목과 판매량 같이 보여줘",
        ]
        assert [entry["question"] for entry in result["ordering"]] == [
            "오늘 납품예정일 기준 확정 발주 수량 상위 품목",
            "오늘 발주수량 대비 확정수량 차이가 큰 품목",
            "오늘 자동발주 비율 알려줘",
        ]
        assert [entry["question"] for entry in result["sales"]] == [
            "오늘 총매출과 일평균 매출 알려줘",
            "오늘 상품 매출 상위 10개 알려줘",
            "오늘 시간대별 매출 피크 알려줘",
        ]

        assert HomeRepository._to_dashboard_display_question(
            "production",
            "오늘 재고 부족(음수 재고) 품목 상위 10개 알려줘",
        ) == "오늘 재고가 부족한 품목 알려줘."
        assert HomeRepository._to_dashboard_display_question(
            "sales",
            "오늘 시간대별 매출 피크 알려줘",
        ) == "오늘 매출이 가장 높은 시간대 알려줘."
    finally:
        csv_path.unlink(missing_ok=True)
