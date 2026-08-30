"""여행 시작 D-1 미확정 코스에 확정 유도 알림을 보내는 DAG.

매일 KST 오전 10시에 트리거하며, 대상 조회(is_confirmed=false + start_date=D+1)와
알림 생성·발송은 모두 BE(CourseConfirmReminderBatchService) 책임이다.
같은 코스 재발송은 BE 의 notification_delivery_guard 쿨다운이 막으므로 재시도해도 안전하다.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import _course_confirm_reminder_tasks as tasks

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "sodosiro",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="course_confirm_reminder_notification",
    description="여행 시작 D-1 미확정 코스 확정 유도 알림 발송",
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2026, 8, 30, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["notification", "course", "batch"],
) as dag:
    send_course_confirm_reminders = PythonOperator(
        task_id="send_course_confirm_reminders",
        python_callable=tasks.send_course_confirm_reminders,
        doc_md="내일 출발인데 아직 확정하지 않은 코스의 소유자에게 확정 유도 알림을 보냅니다.",
    )
