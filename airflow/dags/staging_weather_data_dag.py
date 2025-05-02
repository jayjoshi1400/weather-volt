# airflow/dags/staging_weather_data_dag.py
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
    'staging_weather_data',
    default_args=default_args,
    description='Transform raw NOAA weather data to staging layer',
    schedule_interval='@weekly',  # Weekly schedule, as weather data updates less frequently
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Wait for NOAA data to be loaded
    wait_for_noaa_data = ExternalTaskSensor(
        task_id='wait_for_noaa_data',
        external_dag_id='noaa_weather_history',
        external_task_id='load_to_snowflake',
        timeout=600,
        mode='reschedule',
    )

    # Transform historical weather observations
    transform_weather_data = BashOperator(
        task_id='transform_weather_data',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models staging.stg_weather_historical_hourly --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    # Transform weather station metadata
    transform_station_data = BashOperator(
        task_id='transform_station_data',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models staging.stg_weather_station_metadata --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    # Test historical weather observations
    test_weather_data = BashOperator(
        task_id='test_weather_data',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test --models staging.stg_weather_historical_hourly --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    # Test weather station metadata
    test_station_data = BashOperator(
        task_id='test_station_data',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test --models staging.stg_weather_station_metadata --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )

    wait_for_noaa_data >> [transform_weather_data, transform_station_data]
    transform_weather_data >> test_weather_data
    transform_station_data >> test_station_data
