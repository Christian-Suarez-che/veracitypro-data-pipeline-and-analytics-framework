# Astro + Cosmos Setup Guide

This guide explains how **astronomer-cosmos** integrates dbt Core with Airflow on Astronomer (Astro).

---

## **What is Cosmos?**

**Cosmos** is an Astronomer library that automatically converts your dbt project into Airflow tasks, enabling:
- **Native Airflow Execution**: dbt runs as Airflow tasks (not Docker/Kubernetes operators).
- **Automatic DAG Generation**: Cosmos reads your dbt project and creates a task for each model/test.
- **Dependency Management**: dbt's DAG structure (via `ref()`) becomes Airflow task dependencies.
- **Connection Reuse**: Cosmos uses Airflow connections to authenticate to Snowflake.

**Official Docs**: [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/)

---

## **How Cosmos Works in This Pipeline**

### **1. File Structure**
```
dbt/veracitypro_dbt/
├── dbt_project.yml       # Cosmos reads this to understand your project
├── profiles.yml          # Cosmos uses this to connect to Snowflake
├── models/
│   ├── stg/              # Each .sql file becomes an Airflow task
│   ├── core/
│   └── marts/
└── packages.yml          # Cosmos installs dbt packages if needed
```

### **2. DAG Integration**
In `airflow/dags/vp_daily_batch.py`:

```python
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

dbt_build = DbtTaskGroup(
    group_id="dbt_transform",
    project_config=ProjectConfig(
        dbt_project_path="/path/to/dbt/veracitypro_dbt",
    ),
    profile_config=ProfileConfig(
        profile_name="veracitypro_dbt",
        target_name="prod",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="vp_snowflake",  # Airflow connection
            profile_args={
                "database": "VP_DWH",
                "schema": "STG",
                "warehouse": "WH_INGEST",
            },
        ),
    ),
)
```

**What happens:**
1. Cosmos scans `dbt/veracitypro_dbt/models/`.
2. For each dbt model, it creates an Airflow task.
3. dbt dependencies (`{{ ref('...') }}`) become Airflow task dependencies.
4. Cosmos pulls Snowflake credentials from the `vp_snowflake` Airflow connection.

### **3. Task Execution Flow**
```
Snowflake COPY INTO
    ↓
dbt_transform.stg_keepa_price
    ↓
dbt_transform.core_keepa_price_daily
    ↓
dbt_transform.fct_price_history_daily
    ↓
Power BI Refresh
```

Each dbt model runs as a separate Airflow task, with dependencies automatically inferred from dbt's `ref()` and `source()` macros.

---

## **Cosmos Configuration Options**

### **ProjectConfig**
Tells Cosmos where your dbt project lives:
```python
ProjectConfig(
    dbt_project_path="/usr/local/airflow/dbt/veracitypro_dbt",
    models_relative_path="models",  # Default: "models"
    seeds_relative_path="seeds",    # Default: "seeds"
)
```

### **ProfileConfig**
Tells Cosmos how to connect to Snowflake:
```python
ProfileConfig(
    profile_name="veracitypro_dbt",       # Must match dbt_project.yml
    target_name="prod",                   # Must match profiles.yml
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="vp_snowflake",           # Airflow connection ID
        profile_args={
            "database": "VP_DWH",
            "schema": "STG",              # Base schema (dbt appends layer)
            "warehouse": "WH_INGEST",
            "role": "DBT_ROLE",           # Optional override
        },
    ),
)
```

**Connection Mapping:**
- Cosmos reads `vp_snowflake` connection from Airflow.
- Extracts: `account`, `user`, `password` (or `private_key_path`).
- Passes these to dbt as environment variables.

### **ExecutionConfig**
Controls how dbt is executed:
```python
ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",  # Path to dbt binary
    execution_mode="local",           # Run dbt in same worker (default)
)
```

### **Operator Args**
Control dbt behavior:
```python
operator_args={
    "install_deps": True,    # Run `dbt deps` before models
    "full_refresh": False,   # Don't force full refresh
}
```

---

## **Environment Variables for dbt**

Cosmos automatically sets these environment variables when running dbt:

| Variable                  | Source                              |
|---------------------------|-------------------------------------|
| `SNOWFLAKE_ACCOUNT`       | `vp_snowflake` connection           |
| `SNOWFLAKE_USER`          | `vp_snowflake` connection           |
| `SNOWFLAKE_PASSWORD`      | `vp_snowflake` connection (if set)  |
| `SNOWFLAKE_PRIVATE_KEY`   | `vp_snowflake` connection (if set)  |
| `SNOWFLAKE_ROLE`          | `profile_args` or connection        |
| `SNOWFLAKE_DATABASE`      | `profile_args`                      |
| `SNOWFLAKE_WAREHOUSE`     | `profile_args`                      |
| `SNOWFLAKE_SCHEMA`        | `profile_args`                      |

Your `dbt/veracitypro_dbt/profiles.yml` should reference these:
```yaml
veracitypro_dbt:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD', '') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE', 'DBT_ROLE') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE', 'VP_DWH') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'WH_INGEST') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA', 'STG') }}"
```

---

## **How Snowflake COPY Aligns with dbt**

### **Data Flow**
1. **Airbyte → S3**: Raw JSON files land in `s3://vp-raw-dev-us-east-2/env=dev/source=keepa/`.
2. **Airflow → Snowflake COPY INTO**: Data loaded from S3 into `VP_DWH.RAW.KEEPA_RAW`.
3. **dbt Staging (`stg/`)**: Reads from `RAW.KEEPA_RAW`, unpacks JSON, creates typed columns → `STG.stg_keepa_*`.
4. **dbt Core (`core/`)**: Business logic, deduplication, enrichment → `CORE.core_keepa_*`.
5. **dbt Marts (`marts/`)**: Aggregated, denormalized tables for BI → `MART.dim_*`, `MART.fct_*`.

### **CDC with Snowflake Streams**
If you're using Snowflake Streams for CDC:
1. Create a stream on `RAW.KEEPA_RAW`:
   ```sql
   CREATE STREAM RAW.KEEPA_RAW_STREAM ON TABLE RAW.KEEPA_RAW;
   ```
2. In your dbt staging models, read from the stream:
   ```sql
   {{ config(materialized='incremental', unique_key='product_asin') }}
   SELECT * FROM {{ source('raw', 'KEEPA_RAW_STREAM') }}
   WHERE METADATA$ACTION = 'INSERT'
   ```

This ensures only new/changed rows are processed, making dbt runs fast and idempotent.

---

## **Deploying to Astro Cloud**

### **Step 1: Configure Airflow Connections**
In the Astro UI (Deployments → Environment → Connections), create:

1. **`vp_snowflake`** (Type: Snowflake)
   - Account: `<your_account>.snowflakecomputing.com`
   - User: `DBT_USER`
   - Password: `***` (or use Private Key)
   - Role: `DBT_ROLE`
   - Warehouse: `WH_INGEST`
   - Database: `VP_DWH`
   - Schema: `STG`

2. **`airbyte_cloud`** (Type: HTTP)
   - Host: `https://api.airbyte.com`
   - Extra: `{"Authorization": "Bearer YOUR_AIRBYTE_API_KEY"}`

3. **`aws_default`** (Type: AWS)
   - AWS Access Key ID: `***`
   - AWS Secret Access Key: `***`
   - Region: `us-east-2`

4. **`vp_slack_webhook`** (Type: HTTP)
   - Host: `https://hooks.slack.com`
   - Extra: `{"webhook_token": "/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXX"}`

### **Step 2: Set Environment Variables**
In Astro UI (Deployments → Environment → Variables):

| Variable                      | Value                              |
|-------------------------------|------------------------------------|
| `AIRBYTE_KEEPA_CONNECTION_ID` | `<your-airbyte-connection-id>`     |
| `AS_OF_DATE`                  | `auto` (or specific date for backfill) |

### **Step 3: Deploy via GitHub**
1. Commit your code to GitHub.
2. Astro watches your repository (configured in Astro UI).
3. On push to `main`, Astro:
   - Pulls latest code.
   - Installs `requirements-pipeline.txt`.
   - Syncs DAGs to scheduler.
   - Cosmos automatically discovers dbt project.

### **Step 4: Test the DAG**
1. Open Airflow UI in Astro.
2. Trigger `vp_daily_batch` manually.
3. Watch tasks execute:
   ```
   slack_notify_start
   → airbyte_trigger_keepa_sync
   → airbyte_wait_for_sync
   → s3_verify_keepa_data
   → snowflake_copy_into_raw
   → dbt_transform (expands into 30+ tasks)
   → powerbi_refresh_placeholder
   → slack_notify_success
   ```

---

## **Troubleshooting**

### **"dbt command not found"**
**Cause:** dbt is not installed or not in PATH.

**Fix:** Ensure `requirements-pipeline.txt` includes:
```
dbt-core>=1.7.0,<2.0.0
dbt-snowflake>=1.7.0,<2.0.0
```

### **"Profile 'veracitypro_dbt' not found"**
**Cause:** `profiles.yml` not found or profile name mismatch.

**Fix:** Ensure `dbt/veracitypro_dbt/profiles.yml` exists and matches `dbt_project.yml`:
```yaml
name: "veracitypro_dbt"
profile: "veracitypro_dbt"  # Must match!
```

### **"Snowflake authentication failed"**
**Cause:** Airflow connection `vp_snowflake` is misconfigured.

**Fix:** Verify connection in Astro UI. Test with:
```python
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("vp_snowflake")
print(conn.host, conn.login, conn.password)
```

### **"dbt models not found"**
**Cause:** `dbt_project_path` is incorrect.

**Fix:** Use absolute path or path relative to DAG file:
```python
from pathlib import Path
DBT_PROJECT_PATH = Path(__file__).parent.parent.parent / "dbt" / "veracitypro_dbt"
```

---

## **Best Practices**

1. **Version Control Everything**: dbt models, DAGs, and infrastructure scripts.
2. **Test Locally First**: Use `docker-compose.yml` to test Cosmos integration before deploying.
3. **Use dbt Tests**: Add `tests:` in YAML files. Cosmos will run them as Airflow tasks.
4. **Monitor dbt Logs**: Cosmos pushes dbt logs to Airflow task logs. Check them for warnings.
5. **Incremental Models**: Use `materialized='incremental'` + Snowflake Streams for efficiency.
6. **Worker Queues**: Route dbt tasks to dedicated workers if needed:
   ```python
   default_args={"queue": "dbt"}
   ```

---

## **Additional Resources**

- [Astronomer Cosmos Docs](https://astronomer.github.io/astronomer-cosmos/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [Snowflake + dbt Guide](https://docs.snowflake.com/en/user-guide/ecosystem-dbt)
- [Airflow TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
