-- etl_run의 Airflow 실행 식별자를 DAG 범위까지 확장한다.
-- 적용 전 확인: dag_id가 NULL인 행이 없어야 한다.
-- PostgreSQL에서 트랜잭션으로 한 번만 실행한다.

BEGIN;

ALTER TABLE etl_run
    ALTER COLUMN dag_id SET NOT NULL;

ALTER TABLE etl_run
    DROP CONSTRAINT etl_run_pkey;

ALTER TABLE etl_run
    ADD CONSTRAINT etl_run_pkey PRIMARY KEY (dag_id, run_id);

COMMIT;
