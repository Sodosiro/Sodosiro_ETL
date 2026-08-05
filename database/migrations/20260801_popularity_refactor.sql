-- kakao_spot / kakao_spot_image / trending_spot 제거.
-- tourist_spot 기반 spot_popularity 신규 생성.
-- spot_like / spot_embedding FK 정리.

BEGIN;

-- ──────────────────────────────────────────────────────────────────────────────
-- 1. spot_popularity — tourist_spot 기반 인기도 테이블 (신규)
-- ──────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS spot_popularity (
    content_id       BIGINT PRIMARY KEY,
    mention_score    DOUBLE PRECISION NOT NULL DEFAULT 0,
    popularity_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    category_rank    INTEGER,
    rank_tag         VARCHAR(30),
    calculated_at    TIMESTAMP(6)     NOT NULL DEFAULT now(),

    CONSTRAINT fk_spot_popularity_spot
        FOREIGN KEY (content_id) REFERENCES tourist_spot(content_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_spot_popularity_score
    ON spot_popularity (popularity_score DESC);

CREATE INDEX IF NOT EXISTS idx_spot_popularity_rank
    ON spot_popularity (category_rank)
    WHERE category_rank IS NOT NULL;

-- ──────────────────────────────────────────────────────────────────────────────
-- 2. spot_embedding — kakao_spot_id 컬럼 및 20260727 변경분 복원
--    (embedding_id 서로게이트 PK → content_id PK 복원)
-- ──────────────────────────────────────────────────────────────────────────────

ALTER TABLE spot_embedding DROP CONSTRAINT IF EXISTS fk_spot_embedding_kakao;
ALTER TABLE spot_embedding DROP CONSTRAINT IF EXISTS chk_spot_embedding_source;
DROP INDEX IF EXISTS uk_spot_embedding_kakao;
DROP INDEX IF EXISTS uk_spot_embedding_content;
ALTER TABLE spot_embedding DROP COLUMN IF EXISTS kakao_spot_id;

DO $$
DECLARE
    current_pk TEXT;
BEGIN
    SELECT kcu.column_name INTO current_pk
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
       AND tc.table_name      = kcu.table_name
    WHERE tc.table_name      = 'spot_embedding'
      AND tc.constraint_type = 'PRIMARY KEY';

    IF current_pk = 'embedding_id' THEN
        ALTER TABLE spot_embedding DROP CONSTRAINT spot_embedding_pkey;
        ALTER TABLE spot_embedding DROP COLUMN embedding_id;
        ALTER TABLE spot_embedding ALTER COLUMN content_id SET NOT NULL;
        ALTER TABLE spot_embedding ADD CONSTRAINT spot_embedding_pkey PRIMARY KEY (content_id);
    END IF;
END $$;

-- ──────────────────────────────────────────────────────────────────────────────
-- 3. spot_like — kakao_spot_id 컬럼 및 관련 제약 제거
-- ──────────────────────────────────────────────────────────────────────────────

DROP INDEX IF EXISTS uk_like_user_kakao;
ALTER TABLE spot_like DROP CONSTRAINT IF EXISTS fk_like_kakao;
ALTER TABLE spot_like DROP CONSTRAINT IF EXISTS chk_like_source;
DELETE FROM spot_like WHERE content_id IS NULL;
ALTER TABLE spot_like DROP COLUMN IF EXISTS kakao_spot_id;
ALTER TABLE spot_like ALTER COLUMN content_id SET NOT NULL;

-- ──────────────────────────────────────────────────────────────────────────────
-- 4. kakao_spot_image 제거 (kakao_spot FK CASCADE 있으나 명시적 선행 제거)
-- ──────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS kakao_spot_image;

-- ──────────────────────────────────────────────────────────────────────────────
-- 5. kakao_spot 제거
-- ──────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS kakao_spot;

-- ──────────────────────────────────────────────────────────────────────────────
-- 6. trending_spot 제거 (spot_popularity로 대체)
-- ──────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS trending_spot;

COMMIT;
