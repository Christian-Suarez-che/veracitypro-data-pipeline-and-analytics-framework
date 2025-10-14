FROM quay.io/astronomer/astro-runtime:10.4.0

COPY docker/requirements-pipeline.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Put DAGs where Airflow expects them
COPY airflow/dags /usr/local/airflow/dags
# Include your dbt project so Airflow can run dbt CLI inside the container
COPY dbt/veracitypro_dbt /usr/local/airflow/dags/dbt/veracitypro_dbt
# (Optional) helper scripts used by DAGs
COPY airflow/scripts /usr/local/airflow/dags/scripts
