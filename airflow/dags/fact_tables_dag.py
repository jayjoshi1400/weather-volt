# airflow/dags/fact_tables_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fact_tables',
    default_args=default_args,
    description='Create and maintain fact tables for energy analytics',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Wait for dimension tables to be ready
    wait_for_dimensions = ExternalTaskSensor(
        task_id='wait_for_dimensions',
        external_dag_id='dimension_tables',
        external_task_id='test_dim_location',  # The final task in dimension tables DAG
        timeout=600,
        mode='reschedule',
    )

    # Transform electricity demand fact
    transform_fact_electricity_demand = BashOperator(
        task_id='transform_fact_electricity_demand',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models fact.fct_electricity_demand_hourly --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    # Transform weather observations fact
    transform_fact_weather_observations = BashOperator(
        task_id='transform_fact_weather_observations',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models fact.fct_weather_observations_hourly --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    # Transform electricity generation fact
    transform_fact_electricity_generation = BashOperator(
        task_id='transform_fact_electricity_generation',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models fact.fct_electricity_generation_monthly --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    # Transform electricity sales fact
    transform_fact_electricity_sales = BashOperator(
        task_id='transform_fact_electricity_sales',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models fact.fct_electricity_sales_monthly --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )

    # Test fact tables
    test_fact_tables = BashOperator(
        task_id='test_fact_tables',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test --models fact.* --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    ) 

    wait_for_dimensions  >> [transform_fact_electricity_demand,
        transform_fact_weather_observations,
        transform_fact_electricity_generation,
        transform_fact_electricity_sales] >> test_fact_tables

    # transform_fact_electricity_demand >> transform_fact_weather_observations >> transform_fact_electricity_generation >> transform_fact_electricity_sales >> test_fact_tables