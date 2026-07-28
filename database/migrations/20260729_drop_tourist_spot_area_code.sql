-- area_code는 구형 TourAPI areacode 원천값으로 누락이 많아 더 이상 tourist_spot에 보관하지 않는다.
-- 실제 지역 식별은 ldong_regn_code / ldong_signgu_code를 사용한다.

BEGIN;

DROP INDEX IF EXISTS idx_spot_region;
ALTER TABLE tourist_spot DROP COLUMN IF EXISTS area_code;
CREATE INDEX IF NOT EXISTS idx_spot_sigungu ON tourist_spot (sigungu_code);

COMMIT;
