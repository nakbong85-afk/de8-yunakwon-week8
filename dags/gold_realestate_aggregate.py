"""과제 03 — 부동산 실거래가 Gold (집계 + PostgreSQL 적재).

silver DAG(과제 02)가 같은 달의 정제를 끝내면, spark-submit 으로 gold_spark_sql.py 를 실행해
집계 5종을 계산하고 PostgreSQL 테이블 5개에 적재한 뒤, 각 테이블의 행 수를 검증한다.

    gold_realestate_district_avg / gold_realestate_top10 / gold_realestate_size_dist
    gold_realestate_age_avg / gold_realestate_mom_change

흐름
    wait_for_silver (ExternalTaskSensor)
      -> gold_spark_submit (BashOperator + spark-submit, write.jdbc)
      -> verify_row_counts (각 테이블 row count > 0)

Spark 잡은 PythonOperator 가 아니라 spark-submit 으로 실행한다.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "유낙원"

GOLD_TABLES = [
    "gold_realestate_district_avg",
    "gold_realestate_top10",
    "gold_realestate_size_dist",
    "gold_realestate_age_avg",
    "gold_realestate_mom_change",
]

SPARK_SUBMIT_CMD = """
set -euo pipefail
echo "owner={{ params.owner }} dag={{ dag.dag_id }} run={{ logical_date.strftime('%Y%m') }}"
"${SPARK_SUBMIT:-spark-submit}" \
  --master "${SPARK_MASTER_URL:-local[*]}" \
  --name gold_realestate_aggregate \
  --conf spark.driver.host="${SPARK_DRIVER_HOST:-$(hostname)}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g \
  --conf spark.sql.shuffle.partitions=8 \
  "${GOLD_SCRIPT:-/opt/airflow/scripts/q3/gold_spark_sql.py}" \
  --bucket "${REALESTATE_BUCKET:-realestate-yunakwon}"
"""


def verify_row_counts(**context):
    """적재 후 검증: gold 테이블 5개 모두 row count > 0 이어야 한다."""
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("GOLD_PG_HOST", "postgres"),
        port=os.environ.get("GOLD_PG_PORT", "5432"),
        dbname=os.environ.get("GOLD_PG_DB", "realestate"),
        user=os.environ.get("GOLD_PG_USER", "airflow"),
        password=os.environ.get("GOLD_PG_PASSWORD", "airflow"),
    )
    counts = {}
    try:
        with conn.cursor() as cur:
            for table in GOLD_TABLES:
                cur.execute('SELECT COUNT(*) FROM "%s"' % table)
                counts[table] = cur.fetchone()[0]
                print("%-34s rows = %d" % (table, counts[table]))
    finally:
        conn.close()

    empty = [t for t, n in counts.items() if n <= 0]
    if empty:
        raise AirflowFailException("행이 없는 gold 테이블: %s" % ", ".join(empty))
    print("검증 통과: 테이블 %d개 모두 row count > 0" % len(counts))
    return counts


with DAG(
    dag_id="gold_realestate_aggregate",
    description="과제 03: silver -> 집계 5종 -> PostgreSQL gold 테이블 5개",
    start_date=datetime(2026, 8, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args={"owner": OWNER, "retries": 1, "retry_delay": timedelta(minutes=1)},
    params={"owner": OWNER},
    tags=["week8", "realestate", "gold"],
) as dag:
    # 같은 logical date 의 silver DAG 실행이 성공할 때까지 기다린다
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="silver_realestate_transform",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60 * 6,
    )

    gold_spark_submit = BashOperator(
        task_id="gold_spark_submit",
        bash_command=SPARK_SUBMIT_CMD,
    )

    verify = PythonOperator(task_id="verify_row_counts", python_callable=verify_row_counts)

    wait_for_silver >> gold_spark_submit >> verify
