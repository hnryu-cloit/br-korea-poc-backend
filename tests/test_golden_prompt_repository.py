from app.repositories.sales.golden_prompt_repository import list_golden_prompts


def test_list_golden_prompts_filters_by_domain_and_skips_empty_labels():
    production = list_golden_prompts("production")
    sales = list_golden_prompts("sales")
    ordering = list_golden_prompts("ordering")

    assert len(production) == 5
    assert len(sales) == 5
    assert len(ordering) == 5

    for prompt in production:
        assert prompt["category"] == "생산 관리"
        assert prompt["label"]
        assert prompt["prompt"]
    for prompt in sales:
        assert prompt["category"] == "매출 관리"
    for prompt in ordering:
        assert prompt["category"] == "주문 관리"


def test_list_golden_prompts_without_domain_returns_all_labeled_rows():
    everything = list_golden_prompts(None)
    assert len(everything) == 15
    labels = [prompt["label"] for prompt in everything]
    assert len(labels) == len(set(labels))


def test_list_golden_prompts_unknown_domain_returns_empty():
    assert list_golden_prompts("unknown") == []


def test_sales_service_preserves_golden_prompts_when_ai_overwrites():
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    from app.services.sales_service import SalesService

    repository = MagicMock()
    repository.list_prompts = AsyncMock(return_value=[])

    ai_client = MagicMock()
    ai_client.suggest_sales_prompts = AsyncMock(
        return_value=[
            {"label": "AI 임의 라벨", "category": "기타", "prompt": "AI 가 만든 자유 프롬프트"},
        ]
    )
    service = SalesService(repository=repository, ai_client=ai_client)

    result = asyncio.run(service.list_prompts(domain="production"))

    labels = [item.label for item in result]
    assert "지금 생산해야 할 품목은?" in labels, "골든 라벨이 AI 결과로 덮여 사라졌습니다"
    csv_prompts = {item.prompt for item in result if item.label == "지금 생산해야 할 품목은?"}
    assert any("재고" in p and "부족" in p for p in csv_prompts), "골든 prompt 가 CSV 원문이 아닙니다"
