# Astro Runtime includes Apache Airflow and all necessary dependencies
FROM quay.io/astronomer/astro-runtime:12.1.1

# Note: requirements.txt and packages.txt are automatically installed by Astro Runtime build triggers
# No need to manually COPY or RUN pip install
