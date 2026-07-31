-- 비공개 카카오맵 상세 패널 연동을 제거하고 단일 대표 이미지 구조로 복구한다.
-- 이전 다중 카카오맵 사진은 삭제한다. TourAPI·티스토리 링크는 장소당 하나만 보존한다.

BEGIN;

DELETE FROM kakao_spot_image
WHERE source_type = 'KAKAO_PLACE';

WITH ranked AS (
    SELECT id,
           row_number() OVER (
               PARTITION BY kakao_spot_id
               ORDER BY CASE source_type
                            WHEN 'TOURIST_SPOT' THEN 0
                            WHEN 'TISTORY' THEN 1
                            ELSE 2
                        END,
                        collected_at DESC,
                        id DESC
           ) AS row_num
    FROM kakao_spot_image
)
DELETE FROM kakao_spot_image image
USING ranked
WHERE image.id = ranked.id
  AND ranked.row_num > 1;

ALTER TABLE kakao_spot_image
    DROP CONSTRAINT IF EXISTS uk_kakao_spot_image,
    DROP CONSTRAINT IF EXISTS ck_kakao_spot_image_source_type,
    DROP COLUMN IF EXISTS display_order,
    DROP COLUMN IF EXISTS title,
    DROP COLUMN IF EXISTS registered_at,
    DROP COLUMN IF EXISTS updated_at;

DROP INDEX IF EXISTS uk_kakao_spot_image_source_order;
DROP INDEX IF EXISTS idx_kakao_spot_image_display_order;

ALTER TABLE kakao_spot_image
    ADD CONSTRAINT uk_kakao_spot_image UNIQUE (kakao_spot_id),
    ADD CONSTRAINT ck_kakao_spot_image_source_type
        CHECK (source_type IN ('TOURIST_SPOT', 'TISTORY'));

ALTER TABLE kakao_spot
    DROP COLUMN IF EXISTS homepage,
    DROP COLUMN IF EXISTS business_hours,
    DROP COLUMN IF EXISTS parking_info,
    DROP COLUMN IF EXISTS pet_info,
    DROP COLUMN IF EXISTS detail_info,
    DROP COLUMN IF EXISTS detail_collected_at;

COMMIT;
