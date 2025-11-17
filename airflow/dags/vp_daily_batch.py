"""
VeracityPro Daily Batch Pipeline
Orchestrates: Airbyte → S3 → Snowflake → dbt (via Cosmos) → Power BI
DAG to orchestrate and automate data pipeline
"""

from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow.decorators import dag, task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Slack formatting functions are inline to avoid import issues


# DAG Configuration
DAG_ID = "vp_daily_batch"
AIRBYTE_CONNECTION_ID = os.getenv(
    "AIRBYTE_KEEPA_CONNECTION_ID",
    "Set it in Astro → Deployments → Environment Variables.",
)
S3_BUCKET = "vp-raw-dev-us-east-2"
S3_PREFIX = "env=dev/source=keepa/stream=product_raw/"
# Use relative path from AIRFLOW_HOME (/usr/local/airflow in Astro)
# This resolves to /usr/local/airflow/dbt/veracitypro_dbt in deployed environment
DBT_PROJECT_PATH = Path(__file__).parent.parent / "dbt" / "veracitypro_dbt"


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Daily ELT: Keepa → S3 → Snowflake → dbt → Power BI",
    schedule="10 0 * * *",  # 12:10 AM UTC daily
    start_date=datetime(2025, 11, 15),
    catchup=False,
    tags=["veracitypro", "keepa", "production"],
    max_active_runs=1,
)
def vp_daily_batch():
    # ===== SLACK: START NOTIFICATION =====
    @task
    def notify_start(**context):
        """Send Slack notification that pipeline has started."""
        return {
            "dag_id": context["dag"].dag_id,
            "run_id": context["run_id"],
            "execution_date": context["execution_date"],
        }

    start_info = notify_start()

    slack_start = SlackWebhookOperator(
        task_id="slack_notify_start",
        slack_webhook_conn_id="vp_slack_webhook",
        message="""
:rocket: *Pipeline Started*
• DAG: `{{ dag.dag_id }}`
• Run ID: `{{ run_id }}`
• Execution Date: `{{ execution_date }}`
• Status: _Running_
        """.strip(),
        username="Airflow",
    )

    # ===== AIRBYTE: TRIGGER SYNC =====
    airbyte_trigger = AirbyteTriggerSyncOperator(
        task_id="airbyte_trigger_keepa_sync",
        airbyte_conn_id="airbyte_cloud",
        connection_id=AIRBYTE_CONNECTION_ID,
        asynchronous=True,
        timeout=3600,
        wait_seconds=10,
    )

    # ===== AIRBYTE: WAIT FOR COMPLETION =====
    airbyte_sensor = AirbyteJobSensor(
        task_id="airbyte_wait_for_sync",
        airbyte_conn_id="airbyte_cloud",
        airbyte_job_id="{{ task_instance.xcom_pull(task_ids='airbyte_trigger_keepa_sync', key='job_id') }}",
        timeout=7200,
        poke_interval=60,
    )

    # ===== S3: VERIFY DATA ARRIVAL (OPTIONAL) =====
    # If you want to double-check S3 has files before COPY
    s3_check = S3KeySensor(
        task_id="s3_verify_keepa_data",
        aws_conn_id="aws_default",
        bucket_name=S3_BUCKET,
        bucket_key=f"{S3_PREFIX}*.jsonl.gz",
        wildcard_match=True,
        timeout=600,
        poke_interval=30,
        mode="poke",
    )

    # ===== SNOWFLAKE: COPY INTO RAW =====
    snowflake_copy = SnowflakeOperator(
        task_id="snowflake_copy_into_raw",
        snowflake_conn_id="vp_snowflake",
        sql="""
            -- Use the AIRFLOW_ROLE or DBT_ROLE with appropriate permissions
            USE ROLE DBT_ROLE;
            USE WAREHOUSE WH_INGEST;
            USE DATABASE VP_DWH;
            USE SCHEMA RAW;

            -- COPY data from S3 into KEEPA_RAW table
            COPY INTO KEEPA_RAW (payload, ingest_dt)
            FROM (
                SELECT
                    $1 as payload,
                    CURRENT_DATE() as ingest_dt
                FROM @STAGE_KEEPA
            )
            PATTERN='.*\\.jsonl\\.gz'
            ON_ERROR = 'CONTINUE';

            -- Return row count for logging
            SELECT 'Loaded rows: ' || COUNT(*) as status
            FROM KEEPA_RAW
            WHERE ingest_dt = CURRENT_DATE();
        """,
        autocommit=True,
    )

    # ===== DBT: BUILD MODELS VIA COSMOS =====
    dbt_build = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(
            dbt_project_path=str(DBT_PROJECT_PATH.resolve()),
        ),
        profile_config=ProfileConfig(
            profile_name="veracitypro_dbt",
            target_name="prod",
            profile_mapping=SnowflakeUserPasswordProfileMapping(
                conn_id="vp_snowflake",
                profile_args={
                    "database": "VP_DWH",
                    "schema": "STG",
                    "warehouse": "WH_INGEST",
                },
            ),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/home/astro/.local/bin/dbt",  # dbt installed via requirements.txt
        ),
        operator_args={
            "install_deps": True,
            "full_refresh": False,
        },
        default_args={
            "retries": 2,
            "queue": "dbt",  # Route to dbt worker queue if you have one
        },
    )

    # ===== POWER BI: REFRESH PLACEHOLDER =====
    @task
    def powerbi_refresh_placeholder():
        """
        Placeholder for Power BI dataset refresh.
        Replace with HttpOperator or PowerBIHook when ready.
        """
        print("Power BI refresh step - to be implemented")
        return "success"

    powerbi_task = powerbi_refresh_placeholder()

    # ===== SLACK: SUCCESS NOTIFICATION =====
    @task
    def prepare_success_summary(**context):
        """Gather dbt stats from XCom if available."""
        ti = context["task_instance"]
        # Attempt to fetch dbt summary from Cosmos tasks
        dbt_summary = {}
        try:
            # Cosmos may push metadata to XCom; adapt key as needed
            dbt_summary = (
                ti.xcom_pull(task_ids="dbt_transform", key="dbt_summary") or {}
            )
        except Exception:
            print("Could not fetch dbt summary from XCom")

        return {
            "dag_id": context["dag"].dag_id,
            "run_id": context["run_id"],
            "execution_date": context["execution_date"],
            "dbt_summary": dbt_summary,
        }

    success_info = prepare_success_summary()

    # ===== MONITORING: COLLECT METRICS =====
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def collect_monitoring_metrics(**context):
        ti = context["task_instance"]
        dag_run = context["dag_run"]

        return {
            "dag_id": context["dag"].dag_id,
            "run_id": context["run_id"],
            "execution_date": str(context["execution_date"]),
            "status": dag_run.get_state(),
            "airbyte_job_id": ti.xcom_pull(
                task_ids="airbyte_trigger_keepa_sync", key="job_id"
            ),
            "dbt_summary": ti.xcom_pull(task_ids="dbt_transform", key="dbt_summary")
            or {},
            "rows_loaded_keepa_raw": ti.xcom_pull(
                task_ids="snowflake_copy_into_raw", key="rows_loaded"
            ),
        }

    monitoring_info = collect_monitoring_metrics()
    # inserts monitoring data into the snowflake PIPELINE_RUN_MONITORING table
    monitoring_insert = SnowflakeOperator(
        task_id="monitoring_insert",
        snowflake_conn_id="vp_snowflake",
        sql="""
            USE ROLE AIRFLOW_ROLE;
            USE DATABASE VP_DWH;
            USE SCHEMA MONITORING;
            USE WAREHOUSE WH_INGEST;

            INSERT INTO PIPELINE_RUN_MONITORING (
                dag_id,
                run_id,
                execution_date,
                status,
                airbyte_job_id,
                dbt_models_run,
                dbt_tests_passed,
                dbt_tests_failed,
                rows_loaded_keepa_raw,
                logged_at
            )
            VALUES (
                '{{ ti.xcom_pull(task_ids="collect_monitoring_metrics")["dag_id"] }}',
                '{{ ti.xcom_pull(task_ids="collect_monitoring_metrics")["run_id"] }}',
                '{{ ti.xcom_pull(task_ids="collect_monitoring_metrics")["execution_date"] }}',
                '{{ ti.xcom_pull(task_ids="collect_monitoring_metrics")["status"] }}',
                '{{ ti.xcom_pull(task_ids="collect_monitoring_metrics")["airbyte_job_id"] }}',
                {{ ti.xcom_pull(task_ids="collect_monitoring_metrics")["dbt_summary"].get("models_run", 0) }},
                {{ ti.xcom_pull(task_ids="collect_monitoring_metrics")["dbt_summary"].get("tests_passed", 0) }},
                {{ ti.xcom_pull(task_ids="collect_monitoring_metrics")["dbt_summary"].get("tests_failed", 0) }},
                {{ ti.xcom_pull(task_ids="collect_monitoring_metrics")["rows_loaded_keepa_raw"] or 0 }},
                CURRENT_TIMESTAMP()
            );
        """,
        autocommit=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    slack_success = SlackWebhookOperator(
        task_id="slack_notify_success",
        slack_webhook_conn_id="vp_slack_webhook",
        message="""
:white_check_mark: *Pipeline Completed Successfully*
• DAG: `{{ dag.dag_id }}`
• Run ID: `{{ run_id }}`
• Execution Date: `{{ execution_date }}`
        """.strip(),
        username="Airflow",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ===== SLACK: FAILURE NOTIFICATION =====
    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def slack_notify_failure(**context):
        """Send failure notification to Slack."""
        failed_task_id = None
        error_msg = None

        # Find which task failed
        dag_run = context["dag_run"]
        for task_instance in dag_run.get_task_instances():
            if task_instance.state == "failed":
                failed_task_id = task_instance.task_id
                error_msg = str(task_instance.log_url)
                break

        message = f"""
:x: *Pipeline Failed*
• DAG: `{context["dag"].dag_id}`
• Run ID: `{context["run_id"]}`
• Execution Date: `{context["execution_date"]}`
• Failed Task: `{failed_task_id or "Unknown"}`
• Log URL: {error_msg or "N/A"}
        """.strip()

        SlackWebhookOperator(
            task_id="send_failure_alert",
            slack_webhook_conn_id="vp_slack_webhook",
            message=message,
            username="Airflow",
        ).execute(context=context)

    failure_alert = slack_notify_failure()

    # ===== TASK DEPENDENCIES =====
    # Linear flow with Slack at start/end
    start_info >> slack_start >> airbyte_trigger
    airbyte_trigger >> airbyte_sensor >> s3_check
    s3_check >> snowflake_copy >> dbt_build
    (
        dbt_build
        >> powerbi_task
        >> success_info
        >> monitoring_info
        >> monitoring_insert
        >> slack_success
    )

    # Failure path (runs if ANY task fails)
    (
        [
            airbyte_trigger,
            airbyte_sensor,
            s3_check,
            snowflake_copy,
            dbt_build,
            powerbi_task,
        ]
        >> monitoring_info
        >> monitoring_insert
        >> failure_alert
    )


# Instantiate the DAG
vp_daily_batch_dag = vp_daily_batch()
