-- models/staging/stg_weather_historical_hourly.sql
{{
  config(
    materialized='view',
    schema='STAGING'
  )
}}

WITH source_data AS (
    SELECT
        id AS source_id,
        file_name,
        uploaded_at,
        station_id,
        year,
        raw_data
    FROM {{ source('raw', 'NOAA_ISD_WEATHER_JSON') }}
),

flattened_data AS (
    SELECT
        source_id,
        file_name,
        uploaded_at,
        station_id,
        year,
        f.value AS observation,
        f.index AS observation_index
    FROM source_data,
    LATERAL FLATTEN(input => raw_data) f
),

deduplicated_data AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY station_id, observation:timestamp::TIMESTAMP
            ORDER BY uploaded_at DESC, source_id DESC
        ) as row_num
    FROM flattened_data
    WHERE observation:timestamp::TIMESTAMP IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['station_id', 'observation:timestamp::TIMESTAMP']) }} 
        AS observation_id,
    
    station_id,
    
    observation:timestamp::TIMESTAMP AS timestamp,
    DATE(observation:timestamp::TIMESTAMP) AS date,
    EXTRACT(HOUR FROM observation:timestamp::TIMESTAMP) AS hour_of_day,
    EXTRACT(MONTH FROM observation:timestamp::TIMESTAMP) AS month,
    EXTRACT(YEAR FROM observation:timestamp::TIMESTAMP) AS year,
    
    -- Use correct paths with NULL handling
    observation:raw_data:air_temp_celsius::FLOAT AS temperature_c,
    observation:raw_data:dew_point_celsius::FLOAT AS dew_point_c,
    observation:raw_data:sea_level_pressure_hpa::FLOAT AS pressure_hpa,
    observation:raw_data:wind_speed_mps::FLOAT AS wind_speed_ms,
    
    CASE
        WHEN observation:raw_data:air_temp_celsius::FLOAT IS NOT NULL AND 
             observation:raw_data:air_temp_celsius::FLOAT <= 0 THEN TRUE
        WHEN observation:raw_data:air_temp_celsius::FLOAT IS NOT NULL THEN FALSE
        ELSE NULL -- Handle NULL temperatures
    END AS is_freezing,
    
    CASE
        WHEN observation:raw_data:air_temp_celsius::FLOAT IS NULL THEN 'UNKNOWN'
        WHEN observation:raw_data:air_temp_celsius::FLOAT > 26.7 THEN 'HOT'  -- > 80°F
        WHEN observation:raw_data:air_temp_celsius::FLOAT > 15.6 THEN 'WARM' -- > 60°F
        WHEN observation:raw_data:air_temp_celsius::FLOAT > 4.4 THEN 'MILD'  -- > 40°F
        WHEN observation:raw_data:air_temp_celsius::FLOAT > -6.7 THEN 'COLD' -- > 20°F
        ELSE 'VERY_COLD'
    END AS temperature_category,
    
    -- Calculate degree days with NULL handling
    CASE 
        WHEN temperature_c IS NOT NULL THEN GREATEST(0, 18.3 - temperature_c)
        ELSE NULL
    END AS heating_degree_c,
    
    CASE 
        WHEN temperature_c IS NOT NULL THEN GREATEST(0, temperature_c - 18.3)
        ELSE NULL
    END AS cooling_degree_c,
    
    -- Data quality flag for missing temperature
    CASE WHEN temperature_c IS NULL THEN TRUE ELSE FALSE END AS is_missing_temperature,
    
    source_id,
    file_name,
    uploaded_at AS source_uploaded_at,
    CURRENT_TIMESTAMP() AS loaded_at
    
FROM deduplicated_data
WHERE row_num = 1
