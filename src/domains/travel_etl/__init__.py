"""여행지 공공데이터 ETL 도메인.

공공데이터포털(TourAPI KorService2) → 업무 DB(tourist_spot 등) 적재 파이프라인.
Airflow DAG 는 `controller.TravelEtlController`만 호출하고, 업무 처리는 service 계층이 담당한다.
"""
