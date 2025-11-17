"""
Slack notification utilities for Airflow DAGs.
Uses SlackWebhookOperator to send formatted messages.
"""

from typing import Optional, Dict, Any
from datetime import datetime


def format_dag_start_message(dag_id: str, run_id: str, execution_date: datetime) -> str:
    """Format a DAG start notification message."""
    return f"""
:rocket: *Pipeline Started*
• DAG: `{dag_id}`
• Run ID: `{run_id}`
• Execution Date: `{execution_date.strftime("%Y-%m-%d %H:%M:%S UTC")}`
• Status: _Running_
    """.strip()


def format_dag_success_message(
    dag_id: str,
    run_id: str,
    execution_date: datetime,
    duration_seconds: Optional[float] = None,
    dbt_summary: Optional[Dict[str, Any]] = None,
) -> str:
    """Format a DAG success notification message."""
    duration_str = f" in {duration_seconds:.1f}s" if duration_seconds else ""

    message = f"""
:white_check_mark: *Pipeline Completed Successfully*{duration_str}
• DAG: `{dag_id}`
• Run ID: `{run_id}`
• Execution Date: `{execution_date.strftime("%Y-%m-%d %H:%M:%S UTC")}`
    """.strip()

    if dbt_summary:
        message += "\n\n*dbt Summary:*"
        if "models_run" in dbt_summary:
            message += f"\n• Models: {dbt_summary['models_run']}"
        if "tests_passed" in dbt_summary:
            message += f"\n• Tests Passed: {dbt_summary['tests_passed']}"
        if "tests_failed" in dbt_summary:
            failed = dbt_summary["tests_failed"]
            if failed > 0:
                message += f"\n• :warning: Tests Failed: {failed}"

    return message


def format_dag_failure_message(
    dag_id: str,
    run_id: str,
    execution_date: datetime,
    failed_task: Optional[str] = None,
    error_message: Optional[str] = None,
) -> str:
    """Format a DAG failure notification message."""
    message = f"""
:x: *Pipeline Failed*
• DAG: `{dag_id}`
• Run ID: `{run_id}`
• Execution Date: `{execution_date.strftime("%Y-%m-%d %H:%M:%S UTC")}`
    """.strip()

    if failed_task:
        message += f"\n• Failed Task: `{failed_task}`"

    if error_message:
        # Truncate very long error messages
        error_display = (
            error_message[:500] + "..." if len(error_message) > 500 else error_message
        )
        message += f"\n\n*Error:*\n```{error_display}```"

    return message


def format_task_failure_message(
    dag_id: str, task_id: str, run_id: str, error_message: Optional[str] = None
) -> str:
    """Format a task-level failure notification."""
    message = f"""
:warning: *Task Failed*
• DAG: `{dag_id}`
• Task: `{task_id}`
• Run ID: `{run_id}`
    """.strip()

    if error_message:
        error_display = (
            error_message[:500] + "..." if len(error_message) > 500 else error_message
        )
        message += f"\n\n*Error:*\n```{error_display}```"

    return message
