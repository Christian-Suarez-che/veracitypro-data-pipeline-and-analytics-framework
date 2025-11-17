# Deployment Checklist: VeracityPro Pipeline

Use this checklist before deploying the `vp_daily_batch` DAG to production on Astronomer.

---

## **1. Snowflake Infrastructure**

### **Roles & Users**
- [ ] Run `infra/snowflake/00_roles_users.sql` to create:
  - `AIRBYTE_ROLE`
  - `DBT_ROLE`
  - `AIRFLOW_ROLE`
  - `ANALYST_ROLE`
  - `DS` (Data Science role)

### **Database & Schemas**
- [ ] Run `infra/snowflake/01_db_schemas.sql` to create:
  - Database: `VP_DWH`
  - Schemas: `RAW`, `STG`, `CORE`, `MART`, `ML`, `MONITORING`

### **Warehouses**
- [ ] Run `infra/snowflake/02_warehouses.sql` to create:
  - `WH_INGEST` (for Airbyte, dbt, Airflow)
  - `WH_BI` (for analysts and BI tools)

### **Stages & File Formats**
- [ ] Run `infra/snowflake/03_stages_file_formats.sql` to create:
  - Storage integration: `VP_S3_INT`
  - External stages: `STAGE_KEEPA`, `STAGE_SCRAPERAPI`, `STAGE_SPAPI`
  - File formats: `FF_JSON`, `FF_JSON_NESTED`
  - Landing tables: `KEEPA_RAW`, `SCRAPERAPI_RAW`, `SPAPI_RAW`

- [ ] Verify S3 integration trust:
  ```sql
  DESC INTEGRATION VP_S3_INT;
  ```
  Copy `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` to AWS IAM role trust policy.

### **Grants**
- [ ] Run `infra/snowflake/04_grants.sql` to assign permissions:
  - `DBT_ROLE` can read `RAW`, write to `STG`/`CORE`/`MART`
  - `ANALYST_ROLE` can read `MART`
  - `AIRFLOW_ROLE` can write to `MONITORING`

### **Snowflake Streams (Optional, for CDC)**
- [ ] If using CDC with Snowflake Streams, create:
  ```sql
  USE DATABASE VP_DWH;
  USE SCHEMA RAW;
  CREATE STREAM KEEPA_RAW_STREAM ON TABLE KEEPA_RAW;
  ```
- [ ] Update dbt staging models to read from the stream:
  ```sql
  SELECT * FROM {{ source('raw', 'KEEPA_RAW_STREAM') }}
  WHERE METADATA$ACTION = 'INSERT';
  ```

---

## **2. AWS S3**

### **Bucket Existence**
- [ ] Verify S3 buckets exist:
  - `s3://vp-raw-dev-us-east-2/`
  - `s3://vp-curated-dev-us-east-2/`
  - `s3://vp-monitoring-dev-us-east-2/`

### **IAM Role Trust**
- [ ] Update IAM role `vp-snowflake-s3-role` trust policy with:
  - `STORAGE_AWS_IAM_USER_ARN` (from `DESC INTEGRATION`)
  - `STORAGE_AWS_EXTERNAL_ID` (from `DESC INTEGRATION`)

### **S3 Folder Structure**
- [ ] Verify Airbyte writes to:
  ```
  s3://vp-raw-dev-us-east-2/env=dev/source=keepa/
  ```
- [ ] Ensure at least one test file exists for S3 sensor validation.

---

## **3. Airbyte Cloud**

### **Connection Configuration**
- [ ] Verify Airbyte connection exists for **Keepa → S3**.
- [ ] Note the **Connection ID** (e.g., `f4a8c7b2-...`).
- [ ] Set as environment variable in Astro:
  ```
  AIRBYTE_KEEPA_CONNECTION_ID=<your-connection-id>
  ```

### **API Access**
- [ ] Generate Airbyte Cloud API key.
- [ ] Test API access:
  ```bash
  curl -H "Authorization: Bearer <API_KEY>" \
       https://api.airbyte.com/v1/connections/<CONNECTION_ID>
  ```

---

## **4. dbt Project**

### **Compilation Test**
- [ ] Compile dbt project locally:
  ```bash
  cd dbt/veracitypro_dbt
  dbt compile --profiles-dir . --target prod
  ```
- [ ] Ensure no errors.

### **Model Dependencies**
- [ ] Verify all dbt models use `{{ ref('...') }}` for dependencies.
- [ ] Check `dbt_project.yml` has correct schema mappings:
  ```yaml
  models:
    veracitypro_dbt:
      stg:
        +schema: STG
      core:
        +schema: CORE
      marts:
        +schema: MART
  ```

### **Profiles Configuration**
- [ ] Ensure `dbt/veracitypro_dbt/profiles.yml` references environment variables:
  ```yaml
  account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
  user: "{{ env_var('SNOWFLAKE_USER') }}"
  password: "{{ env_var('SNOWFLAKE_PASSWORD', '') }}"
  role: "{{ env_var('SNOWFLAKE_ROLE', 'DBT_ROLE') }}"
  ```

### **dbt Packages**
- [ ] Run `dbt deps` to install packages:
  ```bash
  dbt deps --profiles-dir . --project-dir .
  ```
- [ ] Verify `dbt_packages/` directory contains `dbt_utils`.

---

## **5. Airflow Connections (Astro)**

Configure the following connections in **Astro UI → Deployments → Environment → Connections**:

### **`vp_snowflake` (Type: Snowflake)**
- [ ] Connection ID: `vp_snowflake`
- [ ] Account: `<account>.snowflakecomputing.com` (e.g., `xy12345.us-east-1`)
- [ ] User: `DBT_USER`
- [ ] Password: `***` (or use Private Key Path for key-based auth)
- [ ] Role: `DBT_ROLE`
- [ ] Warehouse: `WH_INGEST`
- [ ] Database: `VP_DWH`
- [ ] Schema: `STG`

**Test:**
```python
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("vp_snowflake")
print(conn.host, conn.login)
```

### **`airbyte_cloud` (Type: HTTP)**
- [ ] Connection ID: `airbyte_cloud`
- [ ] Host: `https://api.airbyte.com`
- [ ] Extra:
  ```json
  {"Authorization": "Bearer <YOUR_AIRBYTE_API_KEY>"}
  ```

### **`aws_default` (Type: AWS)**
- [ ] Connection ID: `aws_default`
- [ ] AWS Access Key ID: `***`
- [ ] AWS Secret Access Key: `***`
- [ ] Region: `us-east-2`

### **`vp_slack_webhook` (Type: HTTP)**
- [ ] Connection ID: `vp_slack_webhook`
- [ ] Host: `https://hooks.slack.com`
- [ ] Extra:
  ```json
  {"webhook_token": "/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXX"}
  ```

**Test:**
```bash
curl -X POST https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXX \
     -H 'Content-Type: application/json' \
     -d '{"text": "Test from Airflow"}'
```

---

## **6. Environment Variables (Astro)**

Set in **Astro UI → Deployments → Environment → Variables**:

| Variable                      | Value                              | Required |
|-------------------------------|------------------------------------|----------|
| `AIRBYTE_KEEPA_CONNECTION_ID` | `<airbyte-connection-id>`          | Yes      |
| `AS_OF_DATE`                  | `auto` (or specific date)          | Optional |
| `SNOWFLAKE_ACCOUNT`           | `<account>.snowflakecomputing.com` | Optional (if using connection) |
| `SNOWFLAKE_ROLE`              | `DBT_ROLE`                         | Optional (if using connection) |

---

## **7. Python Dependencies**

### **Pipeline Requirements**
- [ ] Ensure `docker/requirements-pipeline.txt` includes:
  ```
  apache-airflow-providers-snowflake>=5.6.0
  apache-airflow-providers-amazon>=8.26.0
  apache-airflow-providers-http>=4.12.0
  apache-airflow-providers-slack>=8.7.0
  apache-airflow-providers-airbyte>=3.8.0
  astronomer-cosmos>=1.5.0
  dbt-core>=1.7.0,<2.0.0
  dbt-snowflake>=1.7.0,<2.0.0
  ```

### **Deployment**
- [ ] Commit `requirements-pipeline.txt` to Git.
- [ ] Astro will auto-install on next deploy.

---

## **8. DAG Validation**

### **Local Testing (Optional)**
- [ ] Test DAG locally with Airflow CLI:
  ```bash
  airflow dags test vp_daily_batch 2024-01-01
  ```

### **Astro Deployment**
- [ ] Push code to GitHub `main` branch.
- [ ] Verify Astro picks up deployment (check Astro UI → Deployments → Deploy History).
- [ ] Open Airflow UI in Astro.
- [ ] Verify `vp_daily_batch` DAG appears.

### **Manual Trigger**
- [ ] Trigger `vp_daily_batch` manually.
- [ ] Watch task logs for:
  - Slack start notification sent
  - Airbyte sync triggered
  - S3 sensor found files
  - Snowflake COPY completed
  - dbt models ran successfully
  - Power BI placeholder executed
  - Slack success notification sent

---

## **9. Slack Notifications**

### **Webhook Configuration**
- [ ] Create Slack webhook in your workspace.
- [ ] Add webhook to Airflow connection `vp_slack_webhook`.
- [ ] Test with sample message:
  ```python
  from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
  SlackWebhookOperator(
      task_id="test_slack",
      slack_webhook_conn_id="vp_slack_webhook",
      message="Test from Airflow",
  ).execute(context={})
  ```

### **Message Formatting**
- [ ] Verify messages appear correctly in Slack:
  - Start notification shows DAG ID, run ID, execution date
  - Success notification includes dbt summary
  - Failure notification includes failed task and error message

---

## **10. Power BI Integration (Future)**

### **Placeholder Task**
- [ ] Current implementation has `powerbi_refresh_placeholder` task.
- [ ] When ready to integrate Power BI:
  1. Create Power BI service principal or user.
  2. Generate API access token.
  3. Replace placeholder with `HttpOperator`:
     ```python
     from airflow.providers.http.operators.http import HttpOperator
     powerbi_refresh = HttpOperator(
         task_id="powerbi_refresh_dataset",
         http_conn_id="powerbi_api",
         endpoint="/v1.0/myorg/datasets/<dataset-id>/refreshes",
         method="POST",
         headers={"Authorization": "Bearer {{ var.value.POWERBI_TOKEN }}"},
     )
     ```

---

## **11. Monitoring & Alerting**

### **Airflow SLAs**
- [ ] (Optional) Add SLAs to critical tasks:
  ```python
  default_args = {
      "sla": timedelta(hours=2),
  }
  ```

### **dbt Test Failures**
- [ ] Ensure dbt tests have `severity: warn` in `dbt_project.yml`:
  ```yaml
  tests:
    +severity: warn
    +store_failures: true
    +schema: MONITORING
  ```
- [ ] Check `VP_DWH.MONITORING` schema for test failure records.

### **Slack Alerts**
- [ ] Verify Slack receives:
  - Start notifications on every run
  - Success notifications with dbt summary
  - Failure notifications with error details

---

## **12. Final Pre-Production Checks**

- [ ] **Code Review**: Have another engineer review DAG code.
- [ ] **Security Audit**: Ensure no hard-coded secrets in code.
- [ ] **Documentation**: Update `README.md` with deployment instructions.
- [ ] **Backup**: Take Snowflake snapshot before first production run.
- [ ] **Dry Run**: Execute DAG once with `schedule=None` (manual trigger only).
- [ ] **Schedule**: Enable daily schedule (`"0 6 * * *"`) after successful dry run.

---

## **13. Post-Deployment**

- [ ] Monitor first 3 scheduled runs.
- [ ] Check Slack for notifications.
- [ ] Verify data in `VP_DWH.MART` schema.
- [ ] Run manual queries to validate data quality.
- [ ] Update runbook with any issues encountered.

---

## **Troubleshooting Resources**

- **Astro Docs**: [docs.astronomer.io](https://docs.astronomer.io)
- **Cosmos Docs**: [astronomer.github.io/astronomer-cosmos](https://astronomer.github.io/astronomer-cosmos/)
- **dbt Docs**: [docs.getdbt.com](https://docs.getdbt.com)
- **Snowflake Docs**: [docs.snowflake.com](https://docs.snowflake.com)

---

## **Emergency Contacts**

| Role              | Contact             |
|-------------------|---------------------|
| Pipeline Owner    | Christian Suarez    |
| Snowflake Admin   | (TBD)               |
| Airbyte Support   | support@airbyte.com |
| Astro Support     | support@astronomer.io |

---

**Last Updated**: [Date]
**Deployment Status**: ☐ Pre-Production | ☐ Production
