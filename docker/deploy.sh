#!/usr/bin/env bash
# Airflow만 새 ETL 환경 변수로 재생성한다. PostgreSQL·Redis·백엔드는 건드리지 않는다.
# 사용법: bash docker/deploy.sh
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f docker/.env ]]; then
  echo "오류: docker/.env 파일이 필요합니다." >&2
  exit 1
fi

compose() {
  docker compose --env-file docker/.env -f docker/docker-compose.yaml "$@"
}

echo "==> [1/2] Airflow scheduler·webserver 재생성"
# --no-deps로 Airflow 외의 컨테이너는 시작·중지·재생성하지 않는다.
compose up -d --force-recreate --no-deps airflow-scheduler airflow-webserver

echo "==> [2/2] 상태 및 최근 로그"
compose ps airflow-scheduler airflow-webserver
compose logs --tail=40 airflow-scheduler airflow-webserver

echo
echo "완료. Airflow UI: http://localhost:8080"
