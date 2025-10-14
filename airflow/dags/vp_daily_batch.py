from datetime import datetime
import json, os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DBT_DIR   = "/usr/local/airflow/dags/dbt/veracitypro_dbt"
SQL_DIR   = "/usr/local/airflow/dags/scripts/copy_sql"
TARGET_DIR= f"{DBT_DIR}/target"

SNOWFLAKE_CONN_ID   = "snowflake_default"
AIRBYTE_HTTP_CONN_ID= "airbyte_api"

API_KEY = f"Bearer {os.environ.get('AIRBYTE_CLOUD_API_TOKEN','')}"
AB_CONN = {
  "keepa": os.environ.get("AIRBYTE_CONN_KEEPA"),
  "scraper_product": os.environ.get("AIRBYTE_CONN_SCRAPER_PRODUCT"),
  "scraper_offers": os.environ.get("AIRBYTE_CONN_SCRAPER_OFFERS"),
  "spapi_orders": os.environ.get("AIRBYTE_CONN_SPAPI_ORDERS"),
}

def summarize_dbt_results(**_):
    path = os.path.join(TARGET_DIR, "run_results.json")
    if not os.path.exists(path):
        raise RuntimeError("run_results.json not found; dbt may have failed.")
    with open(path) as f:
        rr = json.load(f)
    results = rr.get("results", [])
    fails = [r for r in results if r.get("status") in ("error", "fail")]
    warns = [r for r in results if r.get("status") == "warn"]
    print({"total": len(results), "warns": len(warns), "fails": len(fails)})
    if fails:
        raise RuntimeError(f"{len(fails)} dbt tasks failed.")

with DAG(
    dag_id="vp_daily_batch",
    start_date=datetime(2025, 10, 1),
    schedule="10 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "veracitypro"},
    template_searchpath=[SQL_DIR],
    tags=["veracitypro","airbyte","s3","snowflake","dbt"],
) as dag:

    # Optional: trigger Airbyte Cloud syncs
    def ab_trigger(task_id, conn_key):
        return SimpleHttpOperator(
            task_id=task_id,
            http_conn_id=AIRBYTE_HTTP_CONN_ID,
            method="POST",
            endpoint="/v1/jobs",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "veracitypro-airflow",
                "Authorization": API_KEY,
            },
            data=json.dumps({"connectionId": AB_CONN[conn_key], "jobType": "sync"}),
            do_xcom_push=True,
            response_filter=lambda r: r.json()["jobId"],
            log_response=True,
        )

    def ab_wait(task_id, upstream):
        return HttpSensor(
            task_id=task_id,
            http_conn_id=AIRBYTE_HTTP_CONN_ID,
            method="GET",
            endpoint="/v1/jobs/{{ ti.xcom_pull('" + upstream + "') }}",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "veracitypro-airflow",
                "Authorization": API_KEY,
            },
            poke_interval=30,
            timeout=60*60,
            response_check=lambda r: r.json().get("status") == "succeeded",
        )

    # Uncomment if using Airbyte:
    # start_sp   = ab_trigger("ab_start_scraper_product", "scraper_product")
    # start_so   = ab_trigger("ab_start_scraper_offers", "scraper_offers")
    # start_ord  = ab_trigger("ab_start_spapi_orders", "spapi_orders")
    # wait_sp    = ab_wait("ab_wait_scraper_product", "ab_start_scraper_product")
    # wait_so    = ab_wait("ab_wait_scraper_offers",  "ab_start_scraper_offers")
    # wait_ord   = ab_wait("ab_wait_spapi_orders",    "ab_start_spapi_orders")

    copy_scraper_product = SnowflakeOperator(
        task_id="copy_scraper_product_raw",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse="WH_INGEST_XS",
        sql="{{ include('copy_scraper_product.sql') }}",
    )
    copy_scraper_offers = SnowflakeOperator(
        task_id="copy_scraper_offers_raw",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse="WH_INGEST_XS",
        sql="{{ include('copy_scraper_offers.sql') }}",
    )
    copy_spapi_orders = SnowflakeOperator(
        task_id="copy_spapi_orders_raw",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        warehouse="WH_INGEST_XS",
        sql="{{ include('copy_spapi_orders.sql') }}",
    )
    # copy_keepa = SnowflakeOperator(
    #     task_id="copy_keepa_raw",
    #     snowflake_conn_id=SNOWFLAKE_CONN_ID,
    #     warehouse="WH_INGEST_XS",
    #     sql="{{ include('copy_keepa.sql') }}",
    # )

    dbt_build = BashOperator(
        task_id="dbt_build",
        cwd=DBT_DIR,
        bash_command=(
            "export AS_OF_DATE='{{ ds }}' && "
            "dbt deps && "
            "dbt build --vars '{as_of_date: \"{{ ds }}\"}'"
        ),
        env={"DBT_PROFILES_DIR": DBT_DIR},
    )

    summarize = PythonOperator(
        task_id="summarize_dbt_results",
        python_callable=summarize_dbt_results,
    )

    # If using Airbyte, chain waits before COPY:
    # [start_sp, start_so, start_ord] >> [wait_sp, wait_so, wait_ord] >> \
    [copy_scraper_product, copy_scraper_offers, copy_spapi_orders] >> dbt_build >> summarize