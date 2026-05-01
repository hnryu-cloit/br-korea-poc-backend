-- schema_migrations 레거시 항목 표식
--
-- 배경: 과거 짧은 네이밍(`0001_*` ~ `0033_*`)으로 적용됐던 마이그레이션이
-- 이후 긴 네이밍(`20260101_NNNNNN_*`)으로 rename 되면서 동일 SQL이 두 번
-- 적용된 이력이 남았다. migrate_db.py는 파일명(version) 기준으로만 미적용
-- 여부를 판단하므로, rename 이후 두 vertion이 모두 schema_migrations에
-- 기록된 상태이다.
--
-- 정책: 이력은 보존하되, 짧은 네이밍 항목이 레거시임을 식별 가능하도록
-- `is_legacy`와 `legacy_note` 컬럼을 추가하고 표시한다. 신규 마이그레이션
-- 적용 경로(migrate_db.py의 INSERT)는 영향받지 않는다.

ALTER TABLE schema_migrations
    ADD COLUMN IF NOT EXISTS is_legacy BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE schema_migrations
    ADD COLUMN IF NOT EXISTS legacy_note TEXT;

UPDATE schema_migrations
SET
    is_legacy = TRUE,
    legacy_note = COALESCE(
        legacy_note,
        'short-name legacy; superseded by 20260101_NNNNNN_* file rename. Re-applied on 2026-04-29 14:24 caused POC_010 mart data loss.'
    )
WHERE version ~ '^[0-9]{4}[a-z]?_'
  AND is_legacy = FALSE;

COMMENT ON COLUMN schema_migrations.is_legacy IS
    'TRUE면 짧은 네이밍 시절 적용된 레거시 항목. 동일 SQL이 긴 네이밍으로 재적용된 이력 있음.';
COMMENT ON COLUMN schema_migrations.legacy_note IS
    '레거시 항목에 대한 설명. 재적용 시점/영향 범위 등을 기록.';