"""과제 04 — git-sync 동작 확인용 테스트 DAG (BashOperator 만 사용).

이 파일은 GitHub 레포의 dags/ 폴더에만 있고, 클러스터에는 git-sync 가 가져온다.
Airflow UI 목록에 hello_gitsync_dag 가 보이면 git-sync 가 동작한 것이다.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

OWNER = "유낙원"

with DAG(
    dag_id="hello_gitsync_dag",
    description="과제 04: git-sync 로 배포된 테스트 DAG",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": OWNER},
    tags=["week8", "gitsync"],
) as dag:
    hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "hello git-sync from {{ params.owner }} (de-8 yunakwon) on $(hostname) at $(date)"',
        params={"owner": OWNER},
    )

    show_dags = BashOperator(
        task_id="show_synced_dags",
        bash_command='echo "DAGs folder: $AIRFLOW__CORE__DAGS_FOLDER"; ls -l "${AIRFLOW__CORE__DAGS_FOLDER:-/opt/airflow/dags}"',
    )

    hello >> show_dags
