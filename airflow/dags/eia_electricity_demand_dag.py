from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import os
import json
import requests
from pathlib import Path 

# Get configuration from Airflow Variables with defaults
def get_variable_json(var_name, default_value):
    """Get a variable from Airflow variables, or return the default if not found."""
    try:
        var_value = Variable.get(var_name, deserialize_json=True)
        return var_value
    except (ValueError, TypeError):
        try:
            return Variable.get(var_name, default_value)
        except:
            return default_value

DEFAULT_DATA_CONFIG = {
    "base_dir": "/opt/airflow/data",
    "eia_demand_subdir": "eia_demand_raw"
}

DEFAULT_EIA_CONFIG = {
    "balancing_authorities": {
        "CPLE": "EBA.CPLE-ALL.D.H",  # Duke Energy Progress East
        "DUK": "EBA.DUK-ALL.D.H"      # Duke Energy Carolinas
    },
    "retail_sales_params": {
        "frequency": "monthly",
        "data[]": ["sales", "revenue", "price", "customers"],
        "facets[stateid][]": "NC",  # North Carolina
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000  # API maximum
    }, 
    "generation_params": {
    "frequency": "monthly",
    "data[]": ["generation"],  # Required data parameter
    "facets[location][]": "NC",  # Use location instead of stateid
    "start": "2023-01",  # Optional start period
    "end": "2024-12",    # Optional end period
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    "offset": 0,
    "length": 5000
    },
    "api_endpoint": "https://api.eia.gov/v2"
}


DEFAULT_SNOWFLAKE_CONFIG = {
    "database": "ENERGY_DB",
    "schema": "RAW",
    "hourly_demand_table": "EIA_HOURLY_DEMAND_JSON",
    "retail_sales_table": "EIA_RETAIL_SALES_JSON",
    "generation_table": "EIA_ELECTRICITY_GENERATION_JSON",
    "stage_name": "EXTERNAL_STAGE"
}

data_config = DEFAULT_DATA_CONFIG
eia_config = DEFAULT_EIA_CONFIG
snowflake_config = DEFAULT_SNOWFLAKE_CONFIG


# Define data directories using the configuration
DATA_DIR = data_config["base_dir"]
EIA_DEMAND_DIR = f'{DATA_DIR}/{data_config["eia_demand_subdir"]}'

Path(EIA_DEMAND_DIR).mkdir(parents=True, exist_ok=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_eia_hourly_demand():
    """
    Fetches electricity hourly demand data from EIA API for NC
    """
    api_key = Variable.get("eia_api_key")

    balancing_authorities = eia_config["balancing_authorities"]
    api_endpoint = eia_config["api_endpoint"]

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    latest_data_query = f"""
    SELECT MAX(f.value:period::TIMESTAMP) AS latest_timestamp
    FROM {snowflake_config["database"]}.{snowflake_config["schema"]}.{snowflake_config["hourly_demand_table"]},
    LATERAL FLATTEN(input => raw_data:response.data) f
    """
    try:
        latest_timestamp_result = hook.get_first(latest_data_query)
        if latest_timestamp_result and latest_timestamp_result[0]:
            latest_timestamp = latest_timestamp_result[0].strftime("%Y-%m-%dT%H:%M:%SZ")
            print(f"Incremental load starting from: {latest_timestamp}")
        else:
            latest_timestamp = "2023-01-01T00:00:00Z"
            print(f"No existing data, starting from default date: {latest_timestamp}")
    except Exception as e:
        print(f"Error retrieving latest timestamp, defaulting to full load: {str(e)}")
        latest_timestamp = None
    
    successful_fetches = 0
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    for ba_name, series_id in balancing_authorities.items():
        url = f"{api_endpoint}/seriesid/{series_id}?api_key={api_key}"
        if latest_timestamp:
            url += f"&start={latest_timestamp}"
        print(f"Requesting hourly demand data for {ba_name} with URL: {url}")

        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                filename = f'{EIA_DEMAND_DIR}/eia_hourly_demand_{ba_name}_{timestamp}.json'
                with open(filename, 'w') as f:
                    json.dump(data, f, indent=2)
                print(f"Successfully saved hourly demand data for {ba_name}")
                successful_fetches += 1
            else:
                print(f"Error response: {response.text}")
        except Exception as e:
            print(f"Exception when fetching hourly demand data for {ba_name}: {str(e)}")

        metadata = {
        'balancing_authorities': list(balancing_authorities.keys()),
        'series_ids': list(balancing_authorities.values()),
        'timestamp': datetime.now().isoformat(),
        'hourly_demand_pattern': f'eia_hourly_demand_*_{timestamp}.json'
        }

        with open(f'{EIA_DEMAND_DIR}/metadata_demand_{timestamp}.json', 'w') as f:
            json.dump(metadata, f)
        
        return successful_fetches

def fetch_eia_retail_sales():
    """
    Fetches retail sales data from EIA API for North Carolina
    """
    api_key = Variable.get("eia_api_key")
    
    retail_sales_params = eia_config["retail_sales_params"]
    api_endpoint = eia_config["api_endpoint"]
    
    EIA_RETAIL_DIR = f'{DATA_DIR}/eia_retail_raw'
    Path(EIA_RETAIL_DIR).mkdir(parents=True, exist_ok=True)

    # Getting last period in DB for incremental fetching and loading
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    try:
        retail_query = f"""
        SELECT MAX(f.value:period::STRING) AS latest_period 
        FROM {snowflake_config["database"]}.{snowflake_config["schema"]}.{snowflake_config["retail_sales_table"]},
        LATERAL FLATTEN(input => raw_data:response.data) f
        """
        latest_retail_result = hook.get_first(retail_query)
        if latest_retail_result and latest_retail_result[0]:
            latest_retail_period = latest_retail_result[0]
            print(f"Retail sales incremental load starting after period: {latest_retail_period}")
            retail_sales_params["start"] = latest_retail_period
        else:
            print(f"No existing retail sales data, loading all available data")
    except Exception as e:
        print(f"Error retrieving latest retail period, defaulting to full load: {str(e)}")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Fetch retail sales data for NC
    retail_url = f"{api_endpoint}/electricity/retail-sales/data?api_key={api_key}"
    for param, value in retail_sales_params.items():
        if isinstance(value, list):
            for item in value:
                retail_url += f"&{param}={item}"
        else:
            retail_url += f"&{param}={value}"
    
    print(f"Requesting retail sales data for NC with URL: {retail_url}")
    
    try:
        response = requests.get(retail_url)
        
        if response.status_code == 200:
            data = response.json()
            filename = f'{EIA_RETAIL_DIR}/eia_retail_sales_nc_{timestamp}.json'
            
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            
            print(f"Successfully saved retail sales data for NC")
            
            metadata = {
                'retail_sales_state': retail_sales_params.get('facets[stateid][]', 'NC'),
                'timestamp': datetime.now().isoformat(),
                'retail_sales_file': f'eia_retail_sales_nc_{timestamp}.json'
            }
            
            with open(f'{EIA_RETAIL_DIR}/metadata_retail_{timestamp}.json', 'w') as f:
                json.dump(metadata, f)
                
            return 1
        else:
            print(f"Error response: {response.text}")
            return 0
            
    except Exception as e:
        print(f"Exception when fetching retail sales data for NC: {str(e)}")
        return 0


def fetch_eia_generation():
    """
    Fetches electricity generation data from EIA API for North Carolina
    """
    api_key = Variable.get("eia_api_key")
    api_endpoint = eia_config["api_endpoint"]
    generation_params = eia_config["generation_params"]
    
    EIA_GENERATION_DIR = f'{DATA_DIR}/eia_generation_raw'
    Path(EIA_GENERATION_DIR).mkdir(parents=True, exist_ok=True)
    
    # Get the latest period from the database for incremental loading
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    try:
        # Check if table exists first (may not exist on first run)
        table_exists_query = f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{snowflake_config["schema"]}' 
        AND TABLE_NAME = '{snowflake_config["generation_table"]}'
        """
        table_exists = hook.get_first(table_exists_query)[0] > 0
        
        if table_exists:
            latest_data_query = f"""
            SELECT MAX(f.value:period::STRING) AS latest_period 
            FROM {snowflake_config["database"]}.{snowflake_config["schema"]}.{snowflake_config["generation_table"]},
            LATERAL FLATTEN(input => raw_data:response.data) f
            """
            latest_period_result = hook.get_first(latest_data_query)
            
            if latest_period_result and latest_period_result[0]:
                latest_period = latest_period_result[0]
                print(f"Generation data incremental load starting after period: {latest_period}")
                
                generation_params["start"] = latest_period
            else:
                print(f"No existing data in generation table, loading all available data")
        else:
            print("Generation table does not exist yet, loading all available data")
    except Exception as e:
        print(f"Error retrieving latest period for generation data, defaulting to full load: {str(e)}")
    
    # Build the URL with parameters
    generation_url = f"{api_endpoint}/electricity/electric-power-operational-data/data?api_key={api_key}"
    for param, value in generation_params.items():
        if isinstance(value, list):
            for item in value:
                generation_url += f"&{param}={item}"
        else:
            generation_url += f"&{param}={value}"
    
    print(f"Requesting electricity generation data for NC with URL: {generation_url}")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    try:
        response = requests.get(generation_url)
        
        if response.status_code == 200:
            data = response.json()
            print(data)
            filename = f'{EIA_GENERATION_DIR}/eia_generation_nc_{timestamp}.json'
            
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            
            print(f"Successfully saved electricity generation data for NC")
            
            metadata = {
                'state': generation_params.get('facets[stateid][]', 'NC'),
                'timestamp': datetime.now().isoformat(),
                'generation_file': f'eia_generation_nc_{timestamp}.json'
            }
            
            with open(f'{EIA_GENERATION_DIR}/metadata_generation_{timestamp}.json', 'w') as f:
                json.dump(metadata, f)
                
            return 1
        else:
            print(f"Error response: {response.text}")
            return 0
            
    except Exception as e:
        print(f"Exception when fetching generation data for NC: {str(e)}")
        return 0


# Create Snowflake tables to store the raw JSON data
def generate_create_tables_sql():
    database = snowflake_config["database"]
    schema = snowflake_config["schema"]
    hourly_table = snowflake_config["hourly_demand_table"]
    retail_table = snowflake_config["retail_sales_table"]
    generation_table = snowflake_config["generation_table"]
    stage_name = snowflake_config["stage_name"]
    
    return f"""
    CREATE DATABASE IF NOT EXISTS {database};
    CREATE SCHEMA IF NOT EXISTS {database}.{schema};

    CREATE TABLE IF NOT EXISTS {database}.{schema}.{hourly_table} (
        id INTEGER AUTOINCREMENT,
        file_name VARCHAR,
        uploaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        data_date DATE,
        balancing_authority VARCHAR,
        raw_data VARIANT
    );

    CREATE TABLE IF NOT EXISTS {database}.{schema}.{retail_table} (
        id INTEGER AUTOINCREMENT,
        file_name VARCHAR,
        uploaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        data_date DATE,
        raw_data VARIANT
    );
    
    CREATE TABLE IF NOT EXISTS {database}.{schema}.{generation_table} (
        id INTEGER AUTOINCREMENT,
        file_name VARCHAR,
        uploaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        data_date DATE,
        raw_data VARIANT
    );

    CREATE STAGE IF NOT EXISTS {database}.{schema}.{stage_name}
        FILE_FORMAT = (TYPE = 'JSON');
    """

# This function will prepare SQL to load the newest JSON files into Snowflake
def generate_snowflake_load_sql(**context):
    database = snowflake_config["database"]
    schema = snowflake_config["schema"]
    hourly_table = snowflake_config["hourly_demand_table"]
    retail_table = snowflake_config["retail_sales_table"]
    generation_table = snowflake_config["generation_table"]
    stage_name = snowflake_config["stage_name"]
    
    sql_commands = []
    
    # Handle hourly demand data
    EIA_DEMAND_DIR = f'{DATA_DIR}/{data_config["eia_demand_subdir"]}'
    demand_metadata_files = [f for f in os.listdir(EIA_DEMAND_DIR) if f.startswith('metadata_demand_')]
    if demand_metadata_files:
        latest_demand_metadata = sorted(demand_metadata_files)[-1]
        
        with open(f'{EIA_DEMAND_DIR}/{latest_demand_metadata}', 'r') as f:
            demand_metadata = json.load(f)
        
        hourly_demand_pattern = demand_metadata.get('hourly_demand_pattern', '')
        
        # Load hourly demand data
        for ba in demand_metadata.get('balancing_authorities', []):
            specific_file = hourly_demand_pattern.replace('*', ba)
            file_path = f'{EIA_DEMAND_DIR}/{specific_file}'
            
            # Only generate SQL if the file exists
            if os.path.exists(file_path):
                sql = f"""
                PUT file://{file_path} @{database}.{schema}.{stage_name}/eia_demand_raw/ OVERWRITE=TRUE;
                
                INSERT INTO {database}.{schema}.{hourly_table} (file_name, data_date, balancing_authority, raw_data)
                SELECT 
                    '{specific_file}',
                    CURRENT_DATE(),
                    '{ba}',
                    $1
                FROM @{database}.{schema}.{stage_name}/eia_demand_raw/{specific_file};
                """
                sql_commands.append(sql)
            else:
                print(f"Warning: File {file_path} not found, skipping this balancing authority")
    
    # Handle retail sales data
    EIA_RETAIL_DIR = f'{DATA_DIR}/eia_retail_raw'
    if os.path.exists(EIA_RETAIL_DIR):
        retail_metadata_files = [f for f in os.listdir(EIA_RETAIL_DIR) if f.startswith('metadata_retail_')]
        if retail_metadata_files:
            latest_retail_metadata = sorted(retail_metadata_files)[-1]
            
            with open(f'{EIA_RETAIL_DIR}/{latest_retail_metadata}', 'r') as f:
                retail_metadata = json.load(f)
            
            retail_file = retail_metadata.get('retail_sales_file', '')
            
            if retail_file:
                sql = f"""
                PUT file://{EIA_RETAIL_DIR}/{retail_file} @{database}.{schema}.{stage_name}/eia_retail_raw/ OVERWRITE=TRUE;
                
                INSERT INTO {database}.{schema}.{retail_table} (file_name, data_date, raw_data)
                SELECT 
                    '{retail_file}',
                    CURRENT_DATE(),
                    $1
                FROM @{database}.{schema}.{stage_name}/eia_retail_raw/{retail_file};
                """
                sql_commands.append(sql)
    
    # Handle generation data
    EIA_GENERATION_DIR = f'{DATA_DIR}/eia_generation_raw'
    if os.path.exists(EIA_GENERATION_DIR):
        gen_metadata_files = [f for f in os.listdir(EIA_GENERATION_DIR) if f.startswith('metadata_generation_')]
        if gen_metadata_files:
            latest_gen_metadata = sorted(gen_metadata_files)[-1]
            
            with open(f'{EIA_GENERATION_DIR}/{latest_gen_metadata}', 'r') as f:
                gen_metadata = json.load(f)
            
            generation_file = gen_metadata.get('generation_file', '')
            
            if generation_file:
                sql = f"""
                PUT file://{EIA_GENERATION_DIR}/{generation_file} @{database}.{schema}.{stage_name}/eia_generation_raw/ OVERWRITE=TRUE;
                
                INSERT INTO {database}.{schema}.{generation_table} (file_name, data_date, raw_data)
                SELECT 
                    '{generation_file}',
                    CURRENT_DATE(),
                    $1
                FROM @{database}.{schema}.{stage_name}/eia_generation_raw/{generation_file};
                """
                sql_commands.append(sql)
    
    if not sql_commands:
        return "SELECT CURRENT_TIMESTAMP() AS no_files_found;"
        
    return "\n".join(sql_commands)

def generate_simple_test_sql():
    try:
        return generate_snowflake_load_sql()
    except Exception as e:
        print(f"Error generating real SQL: {str(e)}")
        return "SELECT CURRENT_TIMESTAMP() AS fallback_timestamp;"


def explore_eia_endpoint():
    api_key = Variable.get("eia_api_key")
    metadata_url = f"https://api.eia.gov/v2/electricity/electric-power-operational-data?api_key={api_key}"
    response = requests.get(metadata_url)
    print(f"Metadata response: {response.text}")
    return response.json() if response.status_code == 200 else None



with DAG(
    'eia_electricity_data',  
    default_args=default_args,
    description='Fetch electricity data from EIA API and load to Snowflake',
    schedule_interval=timedelta(days=1), 
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Extraction tasks in parallel
    fetch_demand_task = PythonOperator(
        task_id='fetch_eia_hourly_demand',
        python_callable=fetch_eia_hourly_demand,
    )
    
    fetch_retail_task = PythonOperator(
        task_id='fetch_eia_retail_sales',
        python_callable=fetch_eia_retail_sales,
    )
    
    fetch_generation_task = PythonOperator(
        task_id='fetch_eia_generation',
        python_callable=fetch_eia_generation,
    )
    
    create_snowflake_tables = SnowflakeOperator(
        task_id='create_snowflake_tables',
        sql=generate_create_tables_sql(),
        snowflake_conn_id='snowflake_conn',
    )
    
    load_to_snowflake = SnowflakeOperator(
        task_id='load_to_snowflake',
        sql=generate_simple_test_sql(),  
        snowflake_conn_id='snowflake_conn',
    )

    [fetch_demand_task, fetch_retail_task, fetch_generation_task] >> create_snowflake_tables >> load_to_snowflake
    # test_task 
    # fetch_eia_generation