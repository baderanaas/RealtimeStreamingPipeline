import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        # profile_args={"database": "pipe_db", "schema": "PIPE_SCHEMA"},
    ),
)

DBT_PROJECT_DIR = os.path.join(
    os.environ["AIRFLOW_HOME"],
    "dags",
    "dbt",
    "snow_update",
)


dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_DIR),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=os.path.join(
            os.environ["AIRFLOW_HOME"], "etl-env", "bin", "dbt"
        ),
    ),
    schedule_interval="@daily",
    start_date=datetime(2024, 11, 5),
    catchup=False,
    dag_id="DBT_dag",
)

print("AIRFLOW_HOME:", os.environ["AIRFLOW_HOME"])
