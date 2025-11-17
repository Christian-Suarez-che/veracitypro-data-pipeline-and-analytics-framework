# Ideal File Structure for Astro + Cosmos + dbt Pipeline

This document outlines the recommended file structure for a production-ready Astronomer project using Airflow, Cosmos, and dbt Core with Snowflake.

## **Directory Structure**

```
veracitypro-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml                      # CI/CD pipeline (GitHub Actions → Astro)
│
├── airflow/
│   ├── dags/                           # Airflow DAGs
│   │   ├── vp_daily_batch.py          # Main orchestration DAG
│   │   └── vp_keepa_backfill_once.py  # One-time backfill DAG
│   │
│   └── plugins/                        # Custom Airflow plugins
│       └── slack_notifier.py          # Slack notification utilities
│
├── dbt/
│   └── veracitypro_dbt/               # dbt project (executed by Cosmos)
│       ├── models/                     # dbt models
│       │   ├── stg/                   # Staging layer (RAW → STG)
│       │   ├── core/                  # Core business logic (STG → CORE)
│       │   ├── marts/                 # Data marts (CORE → MART)
│       │   └── util/                  # Utility/monitoring views
│       │
│       ├── macros/                    # Reusable Jinja macros
│       ├── tests/                     # Data quality tests
│       ├── dbt_project.yml            # dbt project configuration
│       ├── profiles.yml               # dbt connection profile (for Cosmos)
│       ├── packages.yml               # dbt package dependencies
│       └── package-lock.yml           # Lock file for reproducibility
│
├── docker/
│   ├── pipeline.Dockerfile            # Airflow runtime (optional for Astro)
│   ├── dbt.Dockerfile                 # dbt runtime (optional, if separate)
│   ├── requirements-pipeline.txt      # Python deps (Airflow providers, Cosmos)
│   └── requirements-dbt.txt           # dbt deps (dbt-core, dbt-snowflake)
│
├── infra/
│   ├── snowflake/                     # Snowflake DDL scripts
│   │   ├── 00_roles_users.sql         # Role & user setup
│   │   ├── 01_db_schemas.sql          # Database & schema creation
│   │   ├── 02_warehouses.sql          # Warehouse definitions
│   │   ├── 03_stages_file_formats.sql # S3 stages & file formats
│   │   └── 04_grants.sql              # Permission grants
│   │
│   └── aws/                           # AWS IaC (optional)
│       └── iam_policies/              # S3 & Snowflake integration IAM
│
├── docs/                              # Project documentation
│   ├── ideal_file_structure.md        # This file
│   ├── astro_cosmos_setup_guide.md    # Cosmos integration guide
│   └── deployment_checklist.md        # Pre-deployment checklist
│
├── tests/
│   └── test_smoke.py                  # Basic smoke tests
│
├── .env.example                       # Example environment variables
├── .gitignore                         # Git ignore rules
├── docker-compose.yml                 # Local dev environment (optional)
└── README.md                          # Project overview
```

---

## **Key Directory Explanations**

### **1. `airflow/`**
Contains all Airflow-specific code:
- **`dags/`**: DAG definitions. Each DAG should be self-contained and use the TaskFlow API where possible.
- **`plugins/`**: Custom operators, hooks, sensors, and utilities. Keep these lightweight.

### **2. `dbt/veracitypro_dbt/`**
The dbt project executed by Cosmos:
- **`models/`**: Organized by layer (stg → core → marts) following the [dbt best practices](https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview).
- **`dbt_project.yml`**: Must match the project name referenced in Cosmos.
- **`profiles.yml`**: Used by Cosmos to connect to Snowflake. Pulls credentials from Airflow connections.
- **`packages.yml`**: Dependencies like `dbt_utils`.

### **3. `docker/`**
Python dependencies for Astro:
- **`requirements-pipeline.txt`**: All Airflow providers and Cosmos.
- **`requirements-dbt.txt`**: dbt-core and adapter (dbt-snowflake).

**Note:** In Astro Cloud, you typically merge these into a single `requirements.txt` at the project root. For local dev, keeping them separate is cleaner.

### **4. `infra/`**
Infrastructure-as-code:
- **`snowflake/`**: SQL scripts to bootstrap Snowflake (roles, schemas, stages, grants).
- **`aws/`**: IAM policies for S3 → Snowflake integration.

These should be version-controlled and executed via CI/CD or manually during initial setup.

### **5. `docs/`**
Documentation for developers and operators:
- Architecture diagrams
- Setup guides
- Runbooks and troubleshooting guides

---

## **Files Required vs. Optional**

### **Required Files for Astro + Cosmos**
1. `airflow/dags/vp_daily_batch.py` - Main DAG
2. `dbt/veracitypro_dbt/dbt_project.yml` - dbt project config
3. `dbt/veracitypro_dbt/profiles.yml` - dbt connection profile
4. `docker/requirements-pipeline.txt` - Python dependencies
5. `infra/snowflake/*.sql` - Snowflake setup scripts

### **Optional but Recommended**
1. `airflow/plugins/slack_notifier.py` - Reusable notification logic
2. `dbt/veracitypro_dbt/packages.yml` - dbt package dependencies
3. `tests/test_smoke.py` - Basic DAG validation
4. `.github/workflows/ci.yml` - CI/CD automation
5. `docker-compose.yml` - Local development environment

---

## **Deployment to Astro Cloud**

### **GitHub → Astro Deploy**
1. Push code to GitHub.
2. Astro CLI or GitHub Actions triggers deploy.
3. Astro parses `requirements.txt` and installs dependencies.
4. DAGs are synced to Astro deployment.
5. Cosmos automatically discovers dbt project in `dbt/veracitypro_dbt/`.

### **What Astro Manages**
- Airflow webserver, scheduler, workers
- Python environment (based on `requirements.txt`)
- DAG syncing from Git
- Environment variables and connections

### **What You Manage**
- DAG code and logic
- dbt models and transformations
- Snowflake infrastructure
- Airbyte configurations
- S3 bucket permissions

---

## **Best Practices**

1. **Separation of Concerns**: Keep DAGs thin. Business logic goes in dbt or external libraries.
2. **Idempotency**: Every task should be safe to re-run (use incremental models, COPY with ON_ERROR=CONTINUE).
3. **Configuration as Code**: Use Airflow Variables and Connections, not hard-coded values.
4. **Testing**: Write dbt tests for data quality. Write Airflow tests for DAG integrity.
5. **Monitoring**: Use Slack notifications, dbt test results, and Airflow SLAs.

---

## **Next Steps**

1. Review `docs/astro_cosmos_setup_guide.md` for Cosmos integration details.
2. Review `docs/deployment_checklist.md` before deploying to production.
3. Ensure all Snowflake infrastructure is provisioned (run scripts in `infra/snowflake/`).
4. Configure Airflow connections in Astro UI (`vp_snowflake`, `airbyte_cloud`, `aws_default`, `vp_slack_webhook`).
