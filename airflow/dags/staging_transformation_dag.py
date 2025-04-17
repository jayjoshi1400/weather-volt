# airflow/dags/staging_transformation_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def test_fn():
    user = Variable.get("SNOWFLAKE_USER")
    print(f"SNOWFLAKE_USER: {user}")
    print(f"SNOWFLAKE_ACCOUNT: ", Variable.get("SNOWFLAKE_ACCOUNT"))
    return 0


with DAG(
    'staging_electricity_data',
    default_args=default_args,
    description='Transform raw electricity data to staging layer',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Wait for EIA data to be loaded
    wait_for_eia_data = ExternalTaskSensor(
        task_id='wait_for_eia_data',
        external_dag_id='eia_electricity_data',
        external_task_id='load_to_snowflake',
        timeout=600,
        mode='reschedule',
    )

    # Run dbt to transform electricity demand data
    transform_demand_data = BashOperator(
    task_id='transform_demand_data',
    bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models staging.stg_electricity_demand_hourly --profiles-dir /opt/airflow/dbt',
    env={
        'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
        'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
        'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
    }
    ) 
 
    transform_sales_data = BashOperator(
    task_id='transform_sales_data',
    bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models staging.stg_electricity_sales_monthly --profiles-dir /opt/airflow/dbt',
    env={
        'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
        'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
        'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
    }
    ) 
 
    # Run dbt tests to validate the staging data
    test_demand_data = BashOperator(
        task_id='test_demand_data',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test --models staging.stg_electricity_demand_hourly --profiles-dir /opt/airflow/dbt',
        env={
        'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
        'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
        'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    ) 

    test_sales_data = BashOperator(
        task_id='test_sales_data',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test --models staging.stg_electricity_demand_hourly --profiles-dir /opt/airflow/dbt',
        env={
        'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
        'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
        'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    ) 

    # wait_for_eia_data >> [transform_demand_data, transform_sales_data]
    transform_demand_data >> test_demand_data
    transform_sales_data >> test_sales_data   