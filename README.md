# Sodosiro-ETL

Airflow 기반 **ETL 오케스트레이션 서버**입니다. 여러 ETL 파이프라인을 **도메인 단위로 추가·운영**하며, FastAPI가 API 계층을 담당합니다.

- **오케스트레이션**: Airflow (webserver + scheduler)
- **API 계층**: FastAPI (`src/main.py`) — 설정은 루트 `.env`에서 로드 (`src/core/config.py`)
- **구조**: 도메인 디렉토리 방식 (`src/domains/*`) — ETL 하나 = 도메인 하나

## ETL 파이프라인

새 수집 대상이 생기면 기존 코드를 건드리지 않고 `src/domains/<파이프라인>` 도메인과 DAG를 하나 더 얹는 구조입니다. 각 파이프라인은 독립적으로 스케줄·실행됩니다.

### 현재 파이프라인: 여행지 데이터 ETL

**1. 원천 스냅샷·정규화** — `areaBasedList2` 응답 전체를 CSV로 보존한 뒤, 해당 파일을 읽어 검증·업무 DB UPSERT (변경분만 추림)

목록 스냅샷은 기본적으로 페이지당 500건(`PUBLIC_DATA_PAGE_SIZE=500`)을 요청해 API 호출 횟수를 줄입니다. 공공 API가 이 값을 거절하는 경우 환경변수로 낮춰 조정할 수 있습니다.

**2. 상세 보강** — 변경분에 한해 detailCommon2/detailIntro2/detailInfo2/detailImage2를 독립 pending 큐로 처리

**3. Spring 통지** — 적재가 끝나면 변경된 여행지 content_id 를 Spring(DATA_EXTRACT)에 HTTP 콜백으로 전달.
**AI 처리(대표 키워드·LLM·Spring AI 임베딩 → spot_embedding)는 Spring 책임**이다
(계약: `docs/travel-ai-handoff.md`).

변경분이 없으면 후속 태스크는 `skipped` 처리되어 불필요한 호출을 줄입니다.

```mermaid
flowchart LR
    S[Airflow Scheduler] --> D[여행지 ETL DAG]

    subgraph Python ETL — 수집·적재
        D --> A[공공데이터 수집·정규화]
        A --> C[(업무 DB)]
        A --> DT[상세·이미지 보강]
        DT --> C
    end

    subgraph Spring — AI 처리
        D -->|변경 content_id 통지| SP[Spring DATA_EXTRACT]
        SP -->|Spring AI 키워드·임베딩| C
    end
```

> Airflow 메타데이터 DB와 각 파이프라인의 **업무 DB는 분리**합니다. Airflow 내부 DB를 업무 데이터 저장에 혼용하지 않습니다.
>
> 원천 CSV는 `TRAVEL_SNAPSHOT_DIR`에 논리 실행일별로 하나씩 저장합니다. 같은 실행일의 재시도·CS 재실행은 이미 완료된 스냅샷을 재사용합니다. Docker 기본값은 호스트의 `data/`를 마운트한 `/data/travel/raw`이며, 분산 worker 환경에서는 공유 볼륨 또는 객체 스토리지를 사용해야 합니다.
>
> 여행지 파이프라인의 상세 설계(증분 판별·AI 처리·실패 복구)는 `docs/travel-etl-flow.md`를 참고하세요.

## 폴더 아키텍처

애플리케이션 코드는 `src/`, 배포 관심사는 `docker/`로 분리합니다. ETL 파이프라인은 `src/domains/` 아래에 도메인으로 추가합니다.

```text
sodosiro-ETL/
├── src/                          # 애플리케이션 코드
│   ├── main.py                   # FastAPI 진입점 (uvicorn src.main:app)
│   ├── core/
│   │   └── config.py             # 전역 설정 (.env 로드)
│   └── domains/                  # 도메인별 코드 (ETL 하나 = 도메인 하나)
│       ├── travel_etl/           # 여행지 수집·적재 도메인
│       │   ├── controller/       # Airflow 태스크 진입점·DTO
│       │   ├── service/          # ETL 업무 흐름·정규화·호출 제한
│       │   ├── repository/       # 업무 DB 접근
│       │   ├── client/           # TourAPI·Spring HTTP 통신
│       │   └── config/           # 도메인 설정
│       └── airflow/              # Airflow 오케스트레이션 도메인
│           ├── dags/             # DAG 정의 (스케줄러가 마운트해서 읽음)
│           ├── plugins/          # 커스텀 오퍼레이터·훅·플러그인
│           └── README.md
├── docker/                       # 배포(인프라) 관심사 — 분리
│   ├── docker-compose.yaml       # Airflow webserver·scheduler (메타 DB는 외부 PostgreSQL)
│   ├── .env                      # UID / 로그인 계정 / DB URL (git 제외)
│   └── .env.example              # .env 작성용 템플릿
├── .env                          # FastAPI 앱 설정 (git 제외)
└── .env.example                  # .env 작성용 템플릿
```

- **`domains/airflow`** — 스케줄·DAG·플러그인 등 오케스트레이션 관련 코드를 모아둔 도메인입니다. compose가 이 도메인의 `dags/`, `plugins/`를 컨테이너로 마운트합니다.
- **`domains/travel_etl`** — Airflow 진입점(controller), 업무 흐름(service), DB(repository), 외부 연동(client)을 역할별로 분리한 여행지 ETL 도메인입니다.
- **새 ETL 추가 시** — `src/domains/<이름>/`에 수집·정규화·적재 로직을 두고, `domains/airflow/dags/`에 해당 도메인 함수를 호출하는 얇은 DAG를 추가합니다.

## 실행

### FastAPI (ETL 서버 API)

```bash
uvicorn src.main:app --reload
```

설정은 루트 `.env`에서 읽습니다 (`.env.example` 복사해서 사용). 예: `APP_NAME`, `DEBUG`.

### Airflow (오케스트레이션)

```bash
cd docker
docker compose up airflow-init                              # 최초 1회 (DB 마이그레이션 + admin 계정)
docker compose up -d airflow-webserver airflow-scheduler    # webserver + scheduler
```

- 웹 UI: http://localhost:8080 (기본 로그인 `airflow` / `airflow`, `.env`에서 변경)
- DB 연결은 `docker/.env`의 `AIRFLOW_DB_URL`로 설정합니다 (호스트 PostgreSQL은 `host.docker.internal` 사용).

## 운영 기준 (기본값)

파이프라인별로 조정하되, 기본 원칙은 다음과 같습니다.

| 항목 | 기준 |
| --- | --- |
| 타임존 | DAG에 명시적 `Asia/Seoul` 설정 |
| 동시 실행 | `max_active_runs=1` (실행 겹침 방지) |
| Catchup | `catchup=False`, 백필은 별도 DAG |
| 재시도 | 네트워크·5xx는 지수 백오프, 데이터 검증 실패는 재시도 없이 격리 |
| 비밀값 | Airflow Connection/Secret Backend 사용 (코드에 직접 기록 금지) |
