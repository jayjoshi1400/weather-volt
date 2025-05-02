from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import os
import json
import requests
import pandas as pd
import ftplib
import io
import gzip
from pathlib import Path
import re

# Default configurations
DEFAULT_DATA_CONFIG = {
    "base_dir": "/opt/airflow/data",
    "noaa_history_subdir": "noaa_history_raw"
}

DEFAULT_NOAA_CONFIG = {
    # North Carolina - major cities/airports with good data coverage
    "stations": [
        "723060-13722", # Raleigh-Durham International Airport
        "723140-13881", # Charlotte Douglas International Airport 
        "723170-13723", # Greensboro Piedmont Triad International Airport
        "723150-03812", # Asheville Regional Airport
        "723020-13748"  # Wilmington International Airport
    ],
    "start_year": 2023,  # Start with recent data for efficiency
    "end_year": 2025,    # End year (inclusive)
    "ftp_host": "ftp.ncei.noaa.gov",
    "ftp_dir": "/pub/data/noaa"
}

DEFAULT_SNOWFLAKE_CONFIG = {
    "database": "ENERGY_DB",
    "schema": "RAW",
    "noaa_weather_table": "NOAA_ISD_WEATHER_JSON",
    "stage_name": "EXTERNAL_STAGE"
}

# Get configurations - for now using defaults
data_config = DEFAULT_DATA_CONFIG
noaa_config = DEFAULT_NOAA_CONFIG
snowflake_config = DEFAULT_SNOWFLAKE_CONFIG

# Define data directories
DATA_DIR = data_config["base_dir"]
NOAA_DIR = f'{DATA_DIR}/{data_config["noaa_history_subdir"]}'

# Ensure directory exists
Path(NOAA_DIR).mkdir(parents=True, exist_ok=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def parse_isd_record(line):
    """
    Parse a single line from an ISD file (fixed-width format) into a structured record
    Based on NOAA ISD format documentation
    """
    if len(line) < 105:  # Minimum valid record length
        return None
        
    try:
        # Extract fields based on their fixed positions in the line
        usaf = line[4:10].strip()
        wban = line[10:15].strip()
        
        # Date/time extraction
        year = int(line[15:19])
        month = int(line[19:21])
        day = int(line[21:23])
        hour = int(line[23:25])
        minute = int(line[25:27])
        
        # Create ISO timestamp
        timestamp = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:00Z"
        
        # Position indicators in standard ISD format
        # Extract weather data - these positions are based on ISD documentation
        air_temp_str = line[87:92].strip()
        air_temp = None if air_temp_str == "+9999" else float(air_temp_str) / 10.0
        
        dew_point_str = line[93:98].strip()
        dew_point = None if dew_point_str == "+9999" else float(dew_point_str) / 10.0
        
        # Sea level pressure - position may vary depending on exact format
        # Common positions are around 99-104
        pressure_str = None
        if len(line) >= 104:
            pressure_str = line[99:104].strip()
        pressure = None if not pressure_str or pressure_str == "99999" else float(pressure_str) / 10.0
        
        # Wind speed - position varies by format version
        # Try a few common positions
        wind_speed = None
        for pos in [65, 61, 69]:
            if len(line) >= pos + 5:
                wind_str = line[pos:pos+5].strip()
                if wind_str and wind_str != "99999":
                    try:
                        wind_speed = float(wind_str) / 10.0
                        break
                    except ValueError:
                        pass
        
        # Create record with extracted data
        record = {
            "station": f"{usaf}-{wban}",
            "timestamp": timestamp,
            "raw_data": {
                "timestamp": timestamp,
                "air_temp_celsius": air_temp,
                "dew_point_celsius": dew_point,
                "sea_level_pressure_hpa": pressure,
                "wind_speed_mps": wind_speed
            }
        }
        
        return record
    except Exception as e:
        # More detailed error reporting
        print(f"Error parsing ISD record: {str(e)}")
        print(f"Problematic line: {line[:50]}...")  # Print first 50 chars of the line
        return None


def fetch_noaa_weather_data():
    """
    Downloads and processes NOAA ISD weather data for North Carolina stations
    """
    stations = noaa_config["stations"]
    start_year = noaa_config["start_year"]
    end_year = noaa_config["end_year"]
    ftp_host = noaa_config["ftp_host"]
    ftp_dir = noaa_config["ftp_dir"]
    
    # Track downloaded stations and years
    successful_downloads = 0
    downloaded_files = []
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Check database for latest data to enable incremental loading
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    try:
        table_exists_query = f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{snowflake_config["schema"]}' 
        AND TABLE_NAME = '{snowflake_config["noaa_weather_table"]}'
        """
        table_exists = hook.get_first(table_exists_query)[0] > 0
        
        if table_exists:
            latest_data_query = f"""
            SELECT MAX(raw_data:timestamp::STRING) AS latest_timestamp 
            FROM {snowflake_config["database"]}.{snowflake_config["schema"]}.{snowflake_config["noaa_weather_table"]}
            """
            latest_result = hook.get_first(latest_data_query)
            
            if latest_result and latest_result[0]:
                latest_date = datetime.strptime(latest_result[0], "%Y-%m-%dT%H:%M:%SZ")
                start_year = max(start_year, latest_date.year)
                print(f"Incremental load: Starting from year {start_year} based on latest data")
            else:
                print(f"No existing data in table, loading from {start_year}")
        else:
            print(f"Table doesn't exist yet, will create and load from {start_year}")
    except Exception as e:
        print(f"Error checking latest data: {str(e)}")
    
    print(f"Fetching NOAA ISD data for years {start_year} through {end_year}")
    
    # Connect to FTP server
    try:
        ftp = ftplib.FTP(ftp_host)
        ftp.login()  # Anonymous login
         
        # Process each station for each year
        for station in stations:
            for year in range(start_year, end_year + 1):
                # Parse station ID
                usaf, wban = station.split('-') if '-' in station else (station[:6], station[6:])

                # Ensure proper padding: 6 digits for USAF, 5 for WBAN
                usaf_padded = usaf.zfill(6)
                wban_padded = wban.zfill(5)

                # Two formats to try - with and without separating the components
                station_id = f"{usaf_padded}{wban_padded}"
                formatted_filename = f"{usaf_padded}-{wban_padded}-{year}.gz"

                remote_path = f"{ftp_dir}/{year}/{formatted_filename}"
                local_file = f"{NOAA_DIR}/noaa_isd_{station}_{year}_{timestamp}.json"
                
                print(f"Downloading {remote_path}")
                
                try:
                    # First check if directory exists
                    try:
                        ftp.cwd(f"{ftp_dir}/{year}")
                    except ftplib.error_perm as e:
                        print(f"Directory not found: {ftp_dir}/{year} - {str(e)}")
                        continue
                    
                    # List directory to check what files are actually available
                    files = []
                    ftp.retrlines('LIST', files.append)
                    print(f"Found {len(files)} files in directory. Sample: {files[:3] if files else 'None'}")
                    
                    # Look for our file or similar files
                    patterns_to_try = [
                        f"{usaf_padded}-{wban_padded}-{year}.gz",  # Standard format
                        f"{usaf_padded}{wban_padded}-{year}.gz",   # Combined format
                        f"{usaf_padded}0-{wban_padded}-{year}.gz"  # Some stations have an extra digit
                    ]

                    for pattern in patterns_to_try:
                        potential_matches = [f for f in files if pattern in f]
                        if potential_matches:
                            print(f"Found matching file with pattern {pattern}: {potential_matches}")
                            actual_filename = potential_matches[0].split()[-1]
                            break
                    else:  # No break occurred - no matches found
                        print(f"No matching files found for station {station} using any pattern")
                        continue
                        
                    # Download and process the file
                    with io.BytesIO() as buffer:
                        ftp.retrbinary(f"RETR {actual_filename}", buffer.write)
                        buffer.seek(0)
                        
                        # Decompress and parse the file
                        with gzip.GzipFile(fileobj=buffer) as gz_file:
                            try:
                                content = gz_file.read().decode('utf-8', errors='ignore')
                                
                                # Process each line as standard ISD format
                                records = []
                                for line in content.splitlines():
                                    if len(line) > 105:  # Minimum valid length for ISD format
                                        record = parse_isd_record(line)
                                        if record:
                                            records.append(record)
                                
                                if not records:
                                    print(f"No valid records found in file. Sample line: {content.splitlines()[0][:50]}...")
                            except Exception as e:
                                print(f"Error processing file: {str(e)}")
                                continue
                    
                    # Save records as JSON
                    if records:
                        with open(local_file, 'w') as f:
                            json.dump(records, f)
                        
                        downloaded_files.append({
                            'station': station,
                            'year': year,
                            'file': os.path.basename(local_file),
                            'record_count': len(records)
                        })
                        
                        successful_downloads += 1
                        print(f"Successfully saved {len(records)} records for {station} {year}")
                    else:
                        print(f"No valid records found for {station} {year}")
                
                except ftplib.error_perm as e:
                    print(f"FTP error for {station} {year}: {str(e)}")
                except Exception as e:
                    print(f"Error processing {station} {year}: {str(e)}")
        
        ftp.quit()
    
    except Exception as e:
        print(f"FTP connection error: {str(e)}")
    
    # Create metadata file
    metadata = {
        'stations': stations,
        'years': list(range(start_year, end_year + 1)),
        'timestamp': datetime.now().isoformat(),
        'downloaded_files': downloaded_files
    }
    
    with open(f'{NOAA_DIR}/metadata_noaa_{timestamp}.json', 'w') as f:
        json.dump(metadata, f)
    
    return successful_downloads


def generate_create_table_sql():
    """Generate SQL to create Snowflake table for NOAA weather data"""
    database = snowflake_config["database"]
    schema = snowflake_config["schema"]
    noaa_table = snowflake_config["noaa_weather_table"]
    stage_name = snowflake_config["stage_name"]
    
    return f"""
    CREATE DATABASE IF NOT EXISTS {database};
    CREATE SCHEMA IF NOT EXISTS {database}.{schema};
    
    CREATE OR REPLACE FILE FORMAT {database}.{schema}.noaa_json_format 
    TYPE = 'JSON';
    
    CREATE TABLE IF NOT EXISTS {database}.{schema}.{noaa_table} (
        id INTEGER AUTOINCREMENT,
        file_name VARCHAR,
        uploaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        station_id VARCHAR,
        year INTEGER,
        raw_data VARIANT
    );
    
    CREATE STAGE IF NOT EXISTS {database}.{schema}.{stage_name}
        FILE_FORMAT = (FORMAT_NAME = '{database}.{schema}.noaa_json_format');
    """



def generate_load_sql():
    """Generate SQL to load NOAA weather data into Snowflake"""
    database = snowflake_config["database"]
    schema = snowflake_config["schema"]
    noaa_table = snowflake_config["noaa_weather_table"]
    stage_name = snowflake_config["stage_name"]
    
    # Debugging: Print the directory contents
    print(f"NOAA_DIR contents: {os.listdir(NOAA_DIR)}")
    
    # Get metadata about the files we saved
    metadata_files = [f for f in os.listdir(NOAA_DIR) if f.startswith('metadata_noaa_')]
    print(f"Found {len(metadata_files)} metadata files")
    
    if not metadata_files:
        print("No metadata files found, returning fallback SQL")
        return "SELECT CURRENT_TIMESTAMP() AS no_files_found;"
    
    # Sort by filename (which includes timestamp) to get the latest
    latest_metadata_file = sorted(metadata_files)[-1]
    print(f"Using metadata file: {latest_metadata_file}")
    
    try:
        with open(f'{NOAA_DIR}/{latest_metadata_file}', 'r') as f:
            metadata = json.load(f)
        
        downloaded_files = metadata.get('downloaded_files', [])
        print(f"Found {len(downloaded_files)} downloaded files in metadata")
        
        sql_commands = []
        
        # Process each downloaded file
        for file_info in downloaded_files:
            file_name = file_info.get('file')
            station = file_info.get('station')
            year = file_info.get('year')
            
            if file_name and os.path.exists(f'{NOAA_DIR}/{file_name}'):
                sql = f"""
                PUT file://{NOAA_DIR}/{file_name} @{database}.{schema}.{stage_name}/noaa_raw/ OVERWRITE=TRUE;
                
                INSERT INTO {database}.{schema}.{noaa_table} (file_name, station_id, year, raw_data)
                SELECT 
                    '{file_name}',
                    '{station}',
                    {year},
                    PARSE_JSON($1)
                FROM @{database}.{schema}.{stage_name}/noaa_raw/{file_name} (FILE_FORMAT => {database}.{schema}.noaa_json_format);
                """
                sql_commands.append(sql)
                print(f"Added SQL for file: {file_name}")
        
        if not sql_commands:
            print("No valid SQL commands generated, returning fallback SQL")
            return "SELECT CURRENT_TIMESTAMP() AS no_valid_files_found;"
        
        result = "\n".join(sql_commands)
        print(f"Generated {len(sql_commands)} SQL commands")
        return result
        
    except Exception as e:
        print(f"Error in generate_load_sql: {str(e)}")
        return "SELECT CURRENT_TIMESTAMP() AS error_in_sql_generation;"



def generate_simple_test_sql():
    """Generate SQL with improved error handling"""
    try:
        result = generate_load_sql()
        if not result or result.isspace():
            print("Empty result from generate_load_sql, using fallback")
            return "SELECT CURRENT_TIMESTAMP() AS empty_result_fallback;"
        return result
    except Exception as e:
        print(f"Error generating SQL: {str(e)}")
        return "SELECT CURRENT_TIMESTAMP() AS error_generating_sql;"



# Create DAG
with DAG(
    'noaa_weather_history',
    default_args=default_args,
    description='Fetch historical weather data from NOAA ISD and load to Snowflake',
    schedule_interval=timedelta(days=7),  # Weekly schedule for historical data
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    fetch_weather_task = PythonOperator(
        task_id='fetch_noaa_weather_data',
        python_callable=fetch_noaa_weather_data,
    )
    
    create_snowflake_table = SnowflakeOperator(
        task_id='create_snowflake_table',
        sql=generate_create_table_sql(),
        snowflake_conn_id='snowflake_conn',
    )
    
    load_to_snowflake = SnowflakeOperator(
        task_id='load_to_snowflake',
        sql=generate_simple_test_sql(),
        snowflake_conn_id='snowflake_conn',
    )
    
    # Set task dependencies
    fetch_weather_task >> create_snowflake_table >> load_to_snowflake
    # load_to_snowflake
    # fetch_weather_task