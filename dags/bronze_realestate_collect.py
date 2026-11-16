"""과제 01 — 부동산 실거래가 Bronze (API 수집).

국토교통부 아파트 매매 실거래가 API 를 시군구 6곳에 대해 병렬로 호출하고,
받은 원본 XML 을 그대로 S3 bronze 영역에 저장한다.

    s3://{REALESTATE_BUCKET}/bronze/{yyyymm}/{LAWD_CD}.xml

흐름
    start
      -> collect_by_district (TaskGroup, 시군구 6개 병렬: API 호출 + S3 저장)
      -> check_response (BranchPythonOperator)
            |- collect_success          정상 수집
            |- handle_empty_response    6곳 모두 0건
            `- handle_parse_failure     XML 파싱 실패 또는 API 오류 응답 (DAG 실패 처리)
      -> end

키와 버킷 이름은 코드에 적지 않고 환경 변수로 받는다 (docker-compose.yml 의 x-keys).
    DATA_GO_KR_API_KEY, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REALESTATE_BUCKET
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from urllib.parse import unquote

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

COLLECTOR = "유낙원"

API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
PAGE_SIZE = 1000

# 수집 대상 시군구 (LAWD_CD 5자리)
LAWD_CODES = {
    "11680": "강남구",
    "11650": "서초구",
    "11710": "송파구",
    "11440": "마포구",
    "11170": "용산구",
    "11200": "성동구",
}

GROUP_ID = "collect_by_district"
OK_CODES = {"000", "00"}


def _settings():
    """실행 시점에 환경 변수를 읽는다 (키는 코드에 없음)."""
    api_key = os.environ.get("DATA_GO_KR_API_KEY", "").strip()
    if "%" in api_key:
        # 포털이 보여 주는 키가 URL 인코딩된 형태(%2B, %3D ...)면 원래 값으로 되돌린다.
        # requests 가 보낼 때 다시 인코딩하므로, 그대로 두면 두 번 인코딩되어 인증에 실패한다.
        api_key = unquote(api_key)
    return {
        "api_key": api_key,
        "bucket": os.environ.get("REALESTATE_BUCKET", "realestate-yunakwon"),
        "region": os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"),
    }


def _ensure_bucket(s3, bucket, region):
    """버킷이 없으면 만든다. 다른 계정의 버킷이면 접근 오류를 그대로 알린다."""
    from botocore.exceptions import ClientError

    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code in ("404", "NoSuchBucket", "NotFound"):
            try:
                s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
                print(f"bucket created: {bucket}")
            except ClientError as ce:
                # 시군구 6개 task 가 동시에 만들려다 부딪힌 경우: 다른 task 가 이미 만들었으면 그대로 쓴다
                created_code = str(ce.response.get("Error", {}).get("Code", ""))
                if created_code not in ("BucketAlreadyOwnedByYou", "OperationAborted"):
                    raise
                s3.get_waiter("bucket_exists").wait(Bucket=bucket)
                print(f"bucket already created by another task: {bucket}")
        else:
            raise AirflowFailException(
                f"버킷 {bucket} 에 접근할 수 없습니다 ({code}). REALESTATE_BUCKET 과 AWS 키를 확인하세요."
            )


def _inspect(xml_text):
    """응답 XML 을 살펴 (상태, 건수, 전체건수, 설명) 을 돌려준다.

    상태: ok / empty / api_error / parse_error
    """
    import xml.etree.ElementTree as ET

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        return "parse_error", 0, 0, f"XML 파싱 실패: {e}"

    # 키 오류 등은 <OpenAPI_ServiceResponse> 로 온다
    auth_msg = root.findtext(".//returnAuthMsg")
    if auth_msg:
        return "api_error", 0, 0, f"{auth_msg} ({root.findtext('.//returnReasonCode')})"

    result_code = (root.findtext(".//resultCode") or "").strip()
    if result_code not in OK_CODES:
        return "api_error", 0, 0, f"resultCode={result_code} {root.findtext('.//resultMsg')}"

    count = len(root.findall(".//item"))
    total = int((root.findtext(".//totalCount") or count or 0))
    return ("ok" if count > 0 else "empty"), count, total, "OK"


def collect_district(LAWD_CD, deal_ymd, **context):
    print(f'collector={COLLECTOR}, time={datetime.now()}, lawd={LAWD_CD}')
    import xml.etree.ElementTree as ET

    import boto3
    import requests

    cfg = _settings()
    if not cfg["api_key"]:
        raise AirflowFailException("DATA_GO_KR_API_KEY 가 비어 있습니다. docker-compose.yml 의 x-keys 를 채우세요.")

    def call(page_no):
        params = {
            "serviceKey": cfg["api_key"],
            "LAWD_CD": LAWD_CD,
            "DEAL_YMD": deal_ymd,
            "pageNo": page_no,
            "numOfRows": PAGE_SIZE,
        }
        resp = requests.get(API_URL, params=params, timeout=60)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        return resp.text

    xml_text = call(1)
    status, count, total, message = _inspect(xml_text)
    print(f"lawd={LAWD_CD}({LAWD_CODES.get(LAWD_CD)}) deal_ymd={deal_ymd} status={status} count={count} total={total} msg={message}")

    result = {"lawd": LAWD_CD, "deal_ymd": deal_ymd, "status": status, "count": count, "message": message, "key": None}
    if status in ("parse_error", "api_error"):
        # 저장하지 않고 분기 task 가 실패 경로로 보내도록 상태만 넘긴다
        return result

    # 한 번에 다 오지 않았으면 나머지 페이지의 <item> 을 첫 응답의 <items> 에 이어 붙여 한 파일로 만든다
    if status == "ok" and total > count:
        root = ET.fromstring(xml_text)
        items = root.find(".//items")
        page_no = 1
        while count < total:
            page_no += 1
            more = ET.fromstring(call(page_no)).findall(".//item")
            if not more:
                break
            items.extend(more)
            count += len(more)
        xml_text = ET.tostring(root, encoding="unicode")
        result["count"] = count
        print(f"lawd={LAWD_CD} 여러 페이지를 합침: {page_no} 페이지, {count} 건")

    s3 = boto3.client("s3", region_name=cfg["region"])
    _ensure_bucket(s3, cfg["bucket"], cfg["region"])
    key = f"bronze/{deal_ymd}/{LAWD_CD}.xml"
    s3.put_object(
        Bucket=cfg["bucket"],
        Key=key,
        Body=xml_text.encode("utf-8"),
        ContentType="application/xml; charset=utf-8",
    )
    result["key"] = key
    print(f"saved s3://{cfg['bucket']}/{key} ({len(xml_text.encode('utf-8')):,} bytes, {result['count']} 건)")
    return result


def check_response(**context):
    """수집 결과를 보고 다음 경로를 고른다 (0건 / 파싱 실패 분기)."""
    ti = context["ti"]
    results = [ti.xcom_pull(task_ids=f"{GROUP_ID}.collect_{code}") for code in LAWD_CODES]
    for r in results:
        print(r)

    failed = [r for r in results if not r or r["status"] in ("parse_error", "api_error")]
    total = sum(r["count"] for r in results if r)
    if failed:
        return "handle_parse_failure"
    if total == 0:
        return "handle_empty_response"
    return "collect_success"


def collect_success(**context):
    ti = context["ti"]
    results = [ti.xcom_pull(task_ids=f"{GROUP_ID}.collect_{code}") for code in LAWD_CODES]
    for r in results:
        print(f"{r['lawd']} {LAWD_CODES[r['lawd']]}: {r['count']:>5} 건 -> {r['key']}")
    print(f"합계 {sum(r['count'] for r in results)} 건, 객체 {len(results)} 개")


def handle_empty_response(**context):
    print("6개 시군구 모두 응답 0건입니다. 아직 신고된 거래가 없는 달일 수 있습니다. 원본 XML 은 저장해 두었습니다.")


def handle_parse_failure(**context):
    ti = context["ti"]
    results = [ti.xcom_pull(task_ids=f"{GROUP_ID}.collect_{code}") for code in LAWD_CODES]
    bad = [r for r in results if not r or r["status"] in ("parse_error", "api_error")]
    for r in bad:
        print(f"실패: {r}")
    raise AirflowFailException(f"XML 파싱 실패 또는 API 오류 응답 {len(bad)} 건. 이 실행은 실패로 처리합니다.")


with DAG(
    dag_id="bronze_realestate_collect",
    description="과제 01: 아파트 매매 실거래가 API -> S3 bronze (원본 XML)",
    start_date=datetime(2026, 8, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args={"owner": COLLECTOR, "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["week8", "realestate", "bronze"],
) as dag:
    start = EmptyOperator(task_id="start")

    # 수집 대상 월 = 이 실행의 logical date 가 속한 달 (예: 2026-08-01 실행 -> 202608)
    deal_ymd = "{{ logical_date.strftime('%Y%m') }}"

    with TaskGroup(group_id=GROUP_ID, tooltip="시군구 6곳 병렬 수집") as collect_group:
        for code, name in LAWD_CODES.items():
            PythonOperator(
                task_id=f"collect_{code}",
                python_callable=collect_district,
                op_kwargs={"LAWD_CD": code, "deal_ymd": deal_ymd},
                doc_md=f"{name}({code}) 수집",
            )

    branch = BranchPythonOperator(task_id="check_response", python_callable=check_response)

    success = PythonOperator(task_id="collect_success", python_callable=collect_success)
    empty = PythonOperator(task_id="handle_empty_response", python_callable=handle_empty_response)
    parse_failure = PythonOperator(task_id="handle_parse_failure", python_callable=handle_parse_failure, retries=0)

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    start >> collect_group >> branch >> [success, empty, parse_failure] >> end
