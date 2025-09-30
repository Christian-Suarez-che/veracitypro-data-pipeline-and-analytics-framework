# veracitypro-data-pipeline-and-analytics-framework
Senior capstone project building a cloud-based data pipeline and analytics framework for VeracityPro. VeracityPro is a startup brand focused on designing and selling pickleball paddles through Amazon FBA, with an emphasis on community-building and data-driven decision making.

**Goal:** Scrapers/DB/API → Airbyte → S3 → Snowflake → dbt → Airflow → BI (Power BI/Tableau/Data Science).
<<<<<<< HEAD

**Status:** Week 4 - dbt setup
**Owner:** Christian Suarez
=======
**Status:** Week 3 – Snowflake Setup and Configuration.  
**Owner:** Christian Suarez 
>>>>>>> 323be4322f45b1ca81088ec8584053dcfad4c494

## Architecture

```mermaid
flowchart LR
  subgraph DevOps["Git, GitHub & CI/CD"]
    direction TB
    Git["Git Repository"]
    GitHub["GitHub"]
    CICD["CI/CD & DevOps Pipeline"]
  end

  subgraph Orchestration["Orchestration"]
    Airflow["Apache Airflow"]
    Slack["Slack Notifications"]
  end

  subgraph Collection["Collection"]
    API["API (Amazon Scraper)"]
    PG["PostgreSQL"]
  end

  subgraph Ingestion["Ingestion"]
    Airbyte["Airbyte (Docker)"]
  end

  subgraph DataLake["Data Lake"]
    S3["AMZ S3"]
  end

  subgraph Cleaning["Cleaning"]
    DBT["dbt (Transformations)"]
  end

  subgraph Warehouse["Data Warehouse"]
    Snowflake["Snowflake"]
  end

  subgraph Analytics["Analytics/Dashboards"]
    PowerBI["Power BI"]
    Tableau["Tableau"]
    DS["Data Science (Python, etc.)"]
  end

  %% Data flows
  API -->|Raw Data| Airbyte
  PG -->|Raw Data| Airbyte
  Airbyte -->|Ingest| S3
  S3 -->|Source Data| DBT
  DBT -->|Transformed Data| Snowflake
  Snowflake -->|Warehouse Data| PowerBI
  Snowflake -->|Warehouse Data| Tableau
  Snowflake -->|Warehouse Data| DS

  %% Orchestration
  Airflow -. Orchestrates .-> Airbyte
  Airflow -. Orchestrates .-> S3
  Airflow -. Orchestrates .-> DBT
  Airflow -. Orchestrates .-> Snowflake
  Airflow -. Orchestrates .-> PowerBI
  Airflow -. Orchestrates .-> Tableau
  Airflow -. Orchestrates .-> DS
  Airflow -. Sends alerts .-> Slack

  %% DevOps pipeline
  Git --> GitHub
  GitHub --> CICD
  CICD -. Deploy/Integrate .-> Airbyte
  CICD -. Deploy/Integrate .-> DBT
  CICD -. Deploy/Integrate .-> Snowflake
  CICD -. Deploy/Integrate .-> Airflow
  CICD -. Deploy/Integrate .-> PowerBI
  CICD -. Deploy/Integrate .-> Tableau
  CICD -. Deploy/Integrate .-> DS
  CICD -. Deploy/Integrate .-> S3
```

API -->|Raw Data| Airbyte
  PG -->|Raw Data| Airbyte
  Airbyte -->|Ingest| S3
  S3 -->|Source Data| DBT
  DBT -->|Transformed Data| Snowflake
  Snowflake -->|Warehouse Data| PowerBI & Tableau & DS

  Airflow -. Orchestrates .-> Airbyte & S3 & DBT & Snowflake & PowerBI & Tableau & DS
  Airflow -. Sends alerts .-> Slack

  Git --> GitHub --> CICD
  CICD -. Deploy/Integrate .-> Airbyte & DBT & Snowflake & Airflow & PowerBI & Tableau & DS & S3
