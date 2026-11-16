"""과제 02 — 부동산 실거래가 Silver (PySpark 정제 + UDF).

bronze DAG(과제 01)가 같은 달의 수집을 끝내면, spark-submit 으로 silver_spark.py 를 실행해
원본 XML 을 정제하고 parquet(snappy) 로 저장한다.

    s3://{REALESTATE_BUCKET}/silver/{시군구코드}/yyyymm=XXXXXX/

흐름
    wait_for_bronze (ExternalTaskSensor)  ->  silver_spark_submit (BashOperator + spark-submit)

Spark 잡은 PythonOperator 가 아니라 spark-submit 으로 실행한다.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "유낙원"

SPARK_SUBMIT_CMD = """
set -euo pipefail
echo "owner={{ params.owner }} dag={{ dag.dag_id }} yyyymm={{ logical_date.strftime('%Y%m') }}"
"${SPARK_SUBMIT:-spark-submit}" \
  --master "${SPARK_MASTER_URL:-local[*]}" \
  --name silver_realestate_transform \
  --conf spark.driver.host="${SPARK_DRIVER_HOST:-$(hostname)}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g \
  --conf spark.sql.shuffle.partitions=8 \
  "${SILVER_SCRIPT:-/opt/airflow/scripts/q2/silver_spark.py}" \
  --bucket "${REALESTATE_BUCKET:-realestate-yunakwon}" \
  --yyyymm "{{ logical_date.strftime('%Y%m') }}" \
  --hold-ui-seconds "${SPARK_UI_HOLD_SECONDS:-0}"
"""

with DAG(
    dag_id="silver_realestate_transform",
    description="과제 02: bronze XML -> 정제 + UDF -> S3 silver (parquet, snappy)",
    start_date=datetime(2026, 8, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args={"owner": OWNER, "retries": 1, "retry_delay": timedelta(minutes=1)},
    params={"owner": OWNER},
    tags=["week8", "realestate", "silver"],
) as dag:
    # 같은 logical date 의 bronze DAG 실행이 성공할 때까지 기다린다
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60 * 6,
    )

    silver_spark_submit = BashOperator(
        task_id="silver_spark_submit",
        bash_command=SPARK_SUBMIT_CMD,
    )

    wait_for_bronze >> silver_spark_submit
