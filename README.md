# de8-yunakwon-week8

메타코드 데이터 엔지니어링 8기 · 유낙원 · 8주차 과제 04 (Airflow on Kubernetes) 의 git-sync 대상 레포.

`dags/` 폴더를 helm 차트(apache-airflow/airflow 1.16.0)의 git-sync 가 주기적으로 가져간다.

| 파일 | 내용 |
|---|---|
| `dags/bronze_realestate_collect.py` | 과제 01: 실거래가 API -> S3 bronze |
| `dags/silver_realestate_transform.py` | 과제 02: bronze -> Spark 정제 -> S3 silver |
| `dags/gold_realestate_aggregate.py` | 과제 03: silver -> 집계 -> PostgreSQL gold |
| `dags/hello_gitsync_dag.py` | git-sync 동작 확인용 테스트 DAG (BashOperator) |

API 키와 AWS 키는 이 레포에 없다. DAG 는 실행 시점에 환경 변수에서 읽는다.
