-- 카카오 로컬 검색 API 응답의 전화번호 저장.

BEGIN;

ALTER TABLE kakao_spot
    ADD COLUMN IF NOT EXISTS phone VARCHAR(30);

COMMIT;
