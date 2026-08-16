# ETL 데이터베이스 마이그레이션

이 디렉터리의 SQL은 `travel` 업무 DB에 파일명 순서대로 한 번씩 적용한다. 현재는 자동 마이그레이션 도구가 없으므로, 배포 시 PostgreSQL 클라이언트로 명시적으로 실행한다.

예시(PowerShell):

```powershell
$line = Select-String -LiteralPath 'docker/.env' -Pattern '^TRAVEL_DB_URL=' | Select-Object -First 1
$url = $line.Line.Substring('TRAVEL_DB_URL='.Length)
psql $url -v ON_ERROR_STOP=1 -f database/migrations/20260721_etl_run_composite_key.sql
```

`000_initialize_travel_schema.sql`은 새 DB에서 현재 서버 도메인이 사용하는 전체 테이블과 인덱스를 생성한다. 모든 DDL은 `IF NOT EXISTS`를 사용하므로, 이미 있는 테이블·인덱스와 데이터는 변경하지 않는다.

`20260721_etl_run_composite_key.sql`은 기존 `etl_run`의 기본 키를 `run_id`에서 `(dag_id, run_id)`로 바꾼다. 적용 전에는 `dag_id`가 NULL인 실행 이력을 보정해야 하며, 적용 후 ETL 코드를 함께 배포해야 한다.

`20260727_popular_spot_schema.sql`은 인기 관광지 트렌드 파이프라인(`popular_spot_collection` / `popular_spot_decay` DAG)에 필요한 스키마를 추가한다. 구체적으로는 `trending_spot`, `kakao_spot` 테이블을 신규 생성하고 `spot_embedding`의 PK를 서로게이트 키로 전환해 `kakao_spot_id` FK를 추가한다. **`spot_embedding` 마이그레이션은 기존 PK 구조를 변경하므로 운영 DB에 데이터가 있으면 적용 전 백업을 먼저 수행한다.**

`20260729_trend_source_document.sql`은 카카오 블로그·카페 원천 URL 수집 이력을 추가한다. URL의 복합 유니크 키로 동일 게시글의 재분석과 점수 중복 가산을 방지한다. `20260727_popular_spot_schema.sql` 적용 후 실행한다.

`20260730_kakao_spot_image.sql`은 카카오 인기 장소의 대표 이미지 링크를 추가한다. 기존 `tourist_spot` 이미지가 있으면 그 URL을 우선 연결하고, 없을 때만 `robots.txt`로 접근 가능 여부를 확인한 티스토리 원문의 이미지 URL을 저장한다. 이미지 파일과 원문 HTML은 저장하지 않는다.

`20260731_kakao_spot_detail_and_multiple_images.sql`은 카카오 로컬 검색 API가 응답하는 전화번호를 `kakao_spot`에 저장하기 위한 컬럼을 추가한다.

`20260732_remove_kakao_map_detail_schema.sql`은 이전 비공개 카카오맵 상세 패널 연동에서 만든 다중 사진·상세 정보 구조를 제거한다. `KAKAO_PLACE` 사진 행을 삭제하고, TourAPI 이미지가 있으면 이를 우선해 장소당 단일 이미지 링크만 남긴 뒤 `kakao_spot_id` 단일 유니크 제약을 복구한다.
