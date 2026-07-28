-- 여행 콘텐츠 핵심 테이블의 운영용 설명을 DB 메타데이터에 기록한다.

BEGIN;

COMMENT ON TABLE area_code IS '법정동 시도 코드표 (ldongCode2 원천)';
COMMENT ON COLUMN area_code.area_code IS '법정동 시도 코드';
COMMENT ON COLUMN area_code.name IS '시도명';

COMMENT ON TABLE sigungu_code IS '법정동 시군구 코드표 (ldongCode2 원천)';
COMMENT ON COLUMN sigungu_code.id IS '내부 대리 키';
COMMENT ON COLUMN sigungu_code.area_code IS '상위 법정동 시도 코드 (area_code 참조 값)';
COMMENT ON COLUMN sigungu_code.sigungu_code IS '법정동 시군구 코드';
COMMENT ON COLUMN sigungu_code.name IS '시군구명';

COMMENT ON TABLE tourist_spot IS 'TourAPI areaBasedList2에서 수집한 일반 관광 콘텐츠';
COMMENT ON COLUMN tourist_spot.content_id IS 'TourAPI contentid (외부 자연키)';
COMMENT ON COLUMN tourist_spot.content_type_id IS 'TourAPI 콘텐츠 유형 코드';
COMMENT ON COLUMN tourist_spot.title IS '콘텐츠명';
COMMENT ON COLUMN tourist_spot.addr1 IS '기본 주소';
COMMENT ON COLUMN tourist_spot.addr2 IS '상세 주소';
COMMENT ON COLUMN tourist_spot.zipcode IS '우편번호';
COMMENT ON COLUMN tourist_spot.map_x IS '경도';
COMMENT ON COLUMN tourist_spot.map_y IS '위도';
COMMENT ON COLUMN tourist_spot.map_level IS 'TourAPI 지도 확대 레벨';
COMMENT ON COLUMN tourist_spot.ldong_regn_code IS '법정동 시도 코드';
COMMENT ON COLUMN tourist_spot.ldong_signgu_code IS '법정동 시군구 코드';
COMMENT ON COLUMN tourist_spot.sigungu_code IS '구형 TourAPI 시군구 코드 (원천 누락 가능)';
COMMENT ON COLUMN tourist_spot.category IS '서비스 카테고리: 1=식당, 2=카페, 3=쇼핑, 4=관광지, 5=자연, 6=액티비티, 7=숙박';
COMMENT ON COLUMN tourist_spot.first_image IS '대표 이미지 원본 URL';
COMMENT ON COLUMN tourist_spot.homepage IS '상세 API 홈페이지 정보';
COMMENT ON COLUMN tourist_spot.overview IS '상세 API 소개글';
COMMENT ON COLUMN tourist_spot.infocenter IS '상세 API 문의·안내';
COMMENT ON COLUMN tourist_spot.usetime IS '이용 시간';
COMMENT ON COLUMN tourist_spot.restdate IS '휴무일';
COMMENT ON COLUMN tourist_spot.parking IS '주차 정보';
COMMENT ON COLUMN tourist_spot.expguide IS '체험 안내';
COMMENT ON COLUMN tourist_spot.detail_info IS 'detailInfo2 원천 반복 항목 JSON 배열';
COMMENT ON COLUMN tourist_spot.created_time IS 'TourAPI 원본 생성 시각';
COMMENT ON COLUMN tourist_spot.collected_at IS 'ETL 마지막 수집 시각';

COMMENT ON TABLE spot_image IS '관광 콘텐츠의 detailImage2 이미지 목록';
COMMENT ON COLUMN spot_image.id IS '내부 대리 키';
COMMENT ON COLUMN spot_image.content_id IS '관광 콘텐츠 ID (tourist_spot.content_id)';
COMMENT ON COLUMN spot_image.image_url IS '이미지 URL';
COMMENT ON COLUMN spot_image."order" IS '콘텐츠 내 이미지 순서';

COMMENT ON TABLE spot_embedding IS '관광 콘텐츠 또는 카카오 장소의 벡터 임베딩';
COMMENT ON COLUMN spot_embedding.embedding_id IS '내부 대리 키';
COMMENT ON COLUMN spot_embedding.content_id IS '일반 관광 콘텐츠 ID (tourist_spot.content_id), kakao_spot_id와 상호 배타';
COMMENT ON COLUMN spot_embedding.kakao_spot_id IS '카카오 장소 ID (kakao_spot.id), content_id와 상호 배타';
COMMENT ON COLUMN spot_embedding.embedding IS '1536차원 임베딩 벡터';
COMMENT ON COLUMN spot_embedding.input_text IS '임베딩에 사용한 원문';
COMMENT ON COLUMN spot_embedding.keyword_text IS '검색·추천용 추출 키워드';
COMMENT ON COLUMN spot_embedding.created_at IS '임베딩 생성 시각';

COMMIT;
