# Airflow 도메인

ETL 오케스트레이션(Airflow) 관련 코드를 모아두는 도메인입니다.

```
airflow/
├── dags/         # DAG 정의 (스케줄러가 마운트해서 읽음)
│   └── example_dag.py
└── plugins/      # 커스텀 오퍼레이터 / 훅 / 플러그인
```

## DAG 추가

`dags/`에 `.py` 파일을 추가하면 스케줄러가 자동 감지합니다.

## 실행 (Docker)

Airflow 컨테이너 구성은 배포 관심사라 프로젝트 루트의 [`docker/`](../../../docker/)로 분리되어 있습니다.
compose가 이 도메인의 `dags/`, `plugins/`를 컨테이너에 마운트합니다.

```bash
cd docker
docker compose up airflow-init                              # 최초 1회 (DB 초기화 + admin 계정)
docker compose up -d airflow-webserver airflow-scheduler    # webserver + scheduler
```

- 웹 UI: http://localhost:8080 (로그인 `airflow` / `airflow`, `.env`에서 변경)
