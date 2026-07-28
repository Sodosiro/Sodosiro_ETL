-- TourAPI의 세부 코드(cat1~3, lcls_systm1~3)를 서비스용 7개 정수 카테고리로 통합한다.
-- 1=식당, 2=카페, 3=쇼핑, 4=관광지, 5=자연, 6=액티비티, 7=숙박

BEGIN;

ALTER TABLE tourist_spot ADD COLUMN IF NOT EXISTS category INTEGER;

UPDATE tourist_spot
SET category = CASE
    WHEN lcls_systm1 = 'FD' AND lcls_systm2 = 'FD05' THEN 2
    WHEN lcls_systm1 = 'FD' THEN 1
    WHEN lcls_systm1 = 'SH' THEN 3
    WHEN lcls_systm1 IN ('VE', 'HS', 'C01', 'EV') THEN 4
    WHEN lcls_systm1 = 'NA' THEN 5
    WHEN lcls_systm1 IN ('EX', 'LS') THEN 6
    WHEN lcls_systm1 = 'AC' THEN 7
    ELSE category
END;

ALTER TABLE tourist_spot ALTER COLUMN category SET NOT NULL;
ALTER TABLE tourist_spot
    ADD CONSTRAINT chk_tourist_spot_category CHECK (category BETWEEN 1 AND 7);

DROP INDEX IF EXISTS idx_spot_cat;
DROP INDEX IF EXISTS idx_spot_lcls;
CREATE INDEX IF NOT EXISTS idx_spot_category ON tourist_spot (category);

ALTER TABLE tourist_spot
    DROP COLUMN IF EXISTS cat1,
    DROP COLUMN IF EXISTS cat2,
    DROP COLUMN IF EXISTS cat3,
    DROP COLUMN IF EXISTS lcls_systm1,
    DROP COLUMN IF EXISTS lcls_systm2,
    DROP COLUMN IF EXISTS lcls_systm3;

COMMIT;
