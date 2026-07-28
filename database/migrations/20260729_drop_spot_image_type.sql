-- detailImage2의 imgname은 서비스에서 사용하지 않으므로 저장하지 않는다.

BEGIN;
ALTER TABLE spot_image DROP COLUMN IF EXISTS type;
COMMIT;
