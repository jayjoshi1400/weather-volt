# airflow/dags/analytics_tables_dag.py
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
    'analytics_tables',
    default_args=default_args,
    description='Create and maintain analytics tables for energy data insights',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Wait for fact tables to be ready
    wait_for_facts = ExternalTaskSensor(
        task_id='wait_for_facts',
        external_dag_id='fact_tables',
        external_task_id='test_fact_tables',  # The final task in fact tables DAG
        timeout=600,
        mode='reschedule',
    ) 

    # Standard analytics models
    transform_generation_mix = BashOperator(
        task_id='transform_generation_mix',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models analytics.analytics_generation_mix --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    transform_weather_electricity_correlation = BashOperator(
        task_id='transform_weather_electricity_correlation',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models analytics.analytics_weather_electricity_correlation --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    transform_peak_demand_drivers = BashOperator(
        task_id='transform_peak_demand_drivers',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models analytics.analytics_peak_demand_drivers --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )
    
    # Special analytics models (incremental)
    transform_demand_patterns_cumulative = BashOperator(
        task_id='transform_demand_patterns_cumulative',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models analytics_special.cumulative.analytics_demand_patterns_cumulative --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    )

    
    transform_grid_state_tracking = BashOperator(
        task_id='transform_grid_state_tracking',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --models analytics_special.state_tracking.state_tracking_grid_demand_level --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        } 
    )
    
    # Test analytics tables
    test_analytics_tables = BashOperator(
        task_id='test_analytics_tables',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test --models analytics.* analytics_special.* --profiles-dir /opt/airflow/dbt',
        env={
            'SNOWFLAKE_ACCOUNT': Variable.get("SNOWFLAKE_ACCOUNT"),
            'SNOWFLAKE_USER': Variable.get("SNOWFLAKE_USER"),
            'SNOWFLAKE_PASSWORD': Variable.get("SNOWFLAKE_PASSWORD")
        }
    ) 
    
    # Set dependencies - first do standard tables, then special incremental ones
    # wait_for_facts >> transform_generation_mix >> transform_weather_electricity_correlation >> transform_peak_demand_drivers
    # transform_peak_demand_drivers >> transform_demand_patterns_cumulative >> transform_grid_state_tracking >> test_analytics_tables 

    wait_for_facts >> [transform_generation_mix, transform_weather_electricity_correlation, transform_peak_demand_drivers, transform_peak_demand_drivers,
                        transform_demand_patterns_cumulative, transform_grid_state_tracking] >> test_analytics_tables