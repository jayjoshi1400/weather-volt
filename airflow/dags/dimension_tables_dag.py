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
    'dimension_tables',
    default_args=default_args,
    description='Create and maintain dimension tables for energy analytics',
    schedule_interval='@monthly',  
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Wait for staging data to be available
    wait_for_staging_electricity = ExternalTaskSensor(
        task_id='wait_for_staging_electricity',
        external_dag_id='staging_electricity_data',
        external_task_id='test_sales_data',  # Use the final task in electricity staging DAG
        timeout=600,
        mode='reschedule',
    )
    
    wait_for_staging_weather = ExternalTaskSensor(
        task_id='wait_for_staging_weather',
        external_dag_id='staging_weather_data',
        external_task_id='test_weather_data',  # Use the final task in weather staging DAG
        timeout=600,
        mode='reschedule',
    )

    transform_dim_date = BashOperator(
        task_id='transform_dim_date',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models dimension.dim_date --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )

    # Create location dimension (depends on staging data)
    transform_dim_location = BashOperator(
        task_id='transform_dim_location',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models dimension.dim_location --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    # Create energy source dimension
    transform_dim_energy_source = BashOperator(
        task_id='transform_dim_energy_source',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models dimension.dim_energy_source --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    # Test date dimension
    test_dim_date = BashOperator(
        task_id='test_dim_date',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test --models dimension.dim_date --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    # Test location dimension
    test_dim_location = BashOperator(
        task_id='test_dim_location',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test --models dimension.dim_location --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    # Test energy source dimension
    test_dim_energy_source = BashOperator(
        task_id='test_dim_energy_source',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test --models dimension.dim_energy_source --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    [wait_for_staging_electricity, wait_for_staging_weather] >> transform_dim_location
    
    # Energy source dimension uses the generation staging table
    wait_for_staging_electricity >> transform_dim_energy_source
    
    # Connect tests to their respective transforms
    transform_dim_date >> test_dim_date
    transform_dim_location >> test_dim_location
    transform_dim_energy_source >> test_dim_energy_source
