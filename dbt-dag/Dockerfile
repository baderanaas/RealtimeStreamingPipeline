FROM quay.io/astronomer/astro-runtime:12.4.0

RUN python -m venv etl-env && source etl-env/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate