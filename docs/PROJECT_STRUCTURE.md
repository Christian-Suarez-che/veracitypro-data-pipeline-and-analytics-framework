# VeracityPro Pipeline - Final Project Structure

```
veracitypro-pipeline/
│
├── .github/
│   └── workflows/
│       └── ci.yml                              # GitHub Actions CI/CD
│
├── airflow/
│   ├── dags/
│   │   ├── vp_daily_batch.py                  # ✅ MAIN PRODUCTION DAG
│   │   └── vp_keepa_backfill_once.py          # Backfill DAG (placeholder)
│   │
│   └── plugins/
│       └── slack_notifier.py                   # ✅ NEW - Slack utilities
│
├── dbt/
│   ├── veracitypro_dbt/
│   │   ├── models/
│   │   │   ├── stg/                           # Staging models (RAW → STG)
│   │   │   │   ├── stg_keepa_price.sql
│   │   │   │   ├── stg_keepa_rank.sql
│   │   │   │   ├── stg_keepa_reviews.sql
│   │   │   │   ├── stg_keepa_offers.sql
│   │   │   │   └── ... (more staging models)
│   │   │   │
│   │   │   ├── core/                          # Core business logic
│   │   │   │   ├── core_keepa_price_daily.sql
│   │   │   │   ├── core_keepa_rank_daily.sql
│   │   │   │   ├── core_keepa_reviews_daily.sql
│   │   │   │   ├── core_keepa_product_latest.sql
│   │   │   │   └── ... (more core models)
│   │   │   │
│   │   │   ├── marts/                         # Data marts (CORE → MART)
│   │   │   │   ├── dim_product.sql
│   │   │   │   ├── dim_category.sql
│   │   │   │   ├── dim_seller.sql
│   │   │   │   ├── fct_price_history_daily.sql
│   │   │   │   ├── fct_rank_history_daily.sql
│   │   │   │   └── ... (more marts)
│   │   │   │
│   │   │   └── util/                          # Utility/monitoring views
│   │   │       ├── check_snowflake.sql
│   │   │       ├── list_schemas.sql
│   │   │       └── list_stages.sql
│   │   │
│   │   ├── macros/                            # Jinja macros
│   │   ├── tests/                             # Data quality tests
│   │   ├── dbt_project.yml                    # ✅ UPDATED - Fixed naming
│   │   ├── profiles.yml                       # ✅ NEW - Production profiles
│   │   ├── packages.yml                       # dbt_utils dependency
│   │   └── package-lock.yml                   # Lock file
│   │
│   └── profiles_template.yml                  # Legacy template (deprecated)
│
├── docker/
│   ├── pipeline.Dockerfile                    # Airflow runtime
│   ├── dbt.Dockerfile                         # dbt runtime
│   ├── requirements-pipeline.txt              # ✅ UPDATED - All providers + Cosmos
│   └── requirements-dbt.txt                   # dbt-core + dbt-snowflake
│
├── infra/
│   ├── aws/
│   │   ├── iam_policies/                      # S3 → Snowflake IAM
│   │   └── kms/                               # Encryption keys
│   │
│   └── snowflake/
│       ├── 00_roles_users.sql                 # Roles & users setup
│       ├── 01_db_schemas.sql                  # Database & schemas
│       ├── 02_warehouses.sql                  # Warehouses
│       ├── 03_stages_file_formats.sql         # S3 integration + stages
│       └── 04_grants.sql                      # Permissions
│
├── docs/                                       # ✅ NEW DIRECTORY
│   ├── ideal_file_structure.md                # Project structure guide
│   ├── astro_cosmos_setup_guide.md            # Cosmos integration guide
│   ├── deployment_checklist.md                # Pre-deployment checklist
│   ├── IMPLEMENTATION_SUMMARY.md              # This implementation summary
│   └── PROJECT_STRUCTURE.md                   # This file
│
├── environments/                               # Conda environments
│   ├── vp-analyst.yml
│   ├── vp-analyst.lock.yml
│   ├── vp.yml
│   └── vp.lock.yml
│
├── logs/
│   └── dbt.log                                # dbt execution logs
│
├── tests/
│   └── test_smoke.py                          # Basic smoke tests
│
├── .env.example                               # Example environment variables
├── .gitattributes                             # Git attributes
├── .gitignore                                 # Git ignore rules
├── docker-compose.yml                         # Local dev environment
└── README.md                                  # Project overview
```

---

## **Files Modified in This Implementation**

### **✅ Updated**
1. `airflow/dags/vp_daily_batch.py` - Complete production DAG with Cosmos
2. `dbt/veracitypro_dbt/dbt_project.yml` - Fixed project name and schema references
3. `docker/requirements-pipeline.txt` - Added Airflow providers and Cosmos

### **✅ Created**
1. `airflow/plugins/slack_notifier.py` - Slack notification utilities
2. `dbt/veracitypro_dbt/profiles.yml` - Production-ready dbt profiles
3. `docs/ideal_file_structure.md` - Project structure guide
4. `docs/astro_cosmos_setup_guide.md` - Cosmos setup and configuration
5. `docs/deployment_checklist.md` - Pre-deployment verification
6. `docs/IMPLEMENTATION_SUMMARY.md` - Implementation overview
7. `docs/PROJECT_STRUCTURE.md` - This file

---

## **Key Files for Astro Deployment**

### **Required for DAG Execution**
```
airflow/dags/vp_daily_batch.py          # Main orchestration DAG
airflow/plugins/slack_notifier.py       # Slack utilities
dbt/veracitypro_dbt/dbt_project.yml     # dbt project config
dbt/veracitypro_dbt/profiles.yml        # dbt connection profile
dbt/veracitypro_dbt/models/**/*.sql     # All dbt models
docker/requirements-pipeline.txt        # Python dependencies
```

### **Required for Initial Setup**
```
infra/snowflake/00_roles_users.sql      # Snowflake roles
infra/snowflake/01_db_schemas.sql       # Snowflake schemas
infra/snowflake/02_warehouses.sql       # Snowflake warehouses
infra/snowflake/03_stages_file_formats.sql  # S3 stages
infra/snowflake/04_grants.sql           # Permissions
```

### **Documentation & Guides**
```
docs/deployment_checklist.md            # Pre-deployment checklist
docs/astro_cosmos_setup_guide.md        # Cosmos configuration guide
docs/IMPLEMENTATION_SUMMARY.md          # Implementation overview
README.md                                # Project overview
```

---

## **Deployment Flow**

1. **Setup Infrastructure** (one-time)
   - Run Snowflake SQL scripts (`infra/snowflake/*.sql`)
   - Configure AWS S3 IAM trust relationship
   - Set up Airbyte Cloud connection

2. **Configure Astro** (one-time)
   - Create 4 Airflow connections
   - Set environment variables
   - Deploy code from GitHub

3. **Run Pipeline** (daily)
   - Astro triggers `vp_daily_batch` at 6 AM UTC
   - Pipeline executes: Airbyte → S3 → Snowflake → dbt → Power BI
   - Slack notifications sent at start/end

---

## **Quick Start**

```bash
# 1. Clone repository
git clone <repo-url>
cd veracitypro-pipeline

# 2. Review documentation
cat docs/deployment_checklist.md

# 3. Set up Snowflake
# Run SQL scripts in infra/snowflake/ in order (00, 01, 02, 03, 04)

# 4. Configure Astro
# In Astro UI: Create connections and set environment variables

# 5. Deploy to Astro
git push origin main  # Triggers deploy

# 6. Test the DAG
# In Astro Airflow UI: Trigger vp_daily_batch manually
```

---

## **Support**

- **Documentation**: See `docs/` directory
- **Issues**: Check `docs/deployment_checklist.md` troubleshooting section
- **Astro Support**: support@astronomer.io
- **Pipeline Owner**: Christian Suarez
