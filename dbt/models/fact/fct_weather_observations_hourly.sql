{{
  config(
    materialized = 'incremental',
    unique_key = ['date_id', 'hour_of_day', 'station_id'],
    partition_by = {
      "field": "date",
      "data_type": "date"
    },
    on_schema_change = 'sync_all_columns',
    schema = 'fact'
  )
}}

WITH weather_data AS (
    SELECT
        stg_weather.timestamp,
        DATE(stg_weather.timestamp) AS date,
        stg_weather.hour_of_day,
        stg_weather.station_id,
        stg_weather.temperature_c,
        stg_weather.dew_point_c,
        stg_weather.pressure_hpa,
        stg_weather.wind_speed_ms,
        stg_weather.heating_degree_c,
        stg_weather.cooling_degree_c,
        stg_weather.is_freezing,
        stg_weather.temperature_category,
        stg_weather.source_id,
        stg_weather.source_uploaded_at
    FROM {{ ref('stg_weather_historical_hourly') }} stg_weather
    
    {% if is_incremental() %}
    -- Only process data that's newer than what's already in the fact table
    -- Match both on upload time AND check for specific station/timestamp combinations
    WHERE stg_weather.source_uploaded_at > (
        SELECT MAX(source_uploaded_at) FROM {{ this }}
    )
    AND NOT EXISTS (
        SELECT 1 FROM {{ this }}
        WHERE date_id = TO_CHAR(DATE(stg_weather.timestamp), 'YYYYMMDD')
        AND hour_of_day = stg_weather.hour_of_day
        AND station_id = stg_weather.station_id
    )
    {% endif %}
),

-- Deduplicate weather data first to handle multiple observations for same time/station
deduplicated_weather AS (
    SELECT
        timestamp,
        date,
        hour_of_day,
        station_id,
        temperature_c,
        dew_point_c,
        pressure_hpa,
        wind_speed_ms,
        heating_degree_c,
        cooling_degree_c,
        is_freezing,
        temperature_category,
        source_id,
        source_uploaded_at
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY DATE(timestamp), hour_of_day, station_id
                ORDER BY source_uploaded_at DESC, source_id DESC
            ) as row_num
        FROM weather_data
    )
    WHERE row_num = 1
),

-- Join with dimension tables to get surrogate keys
with_dimension_keys AS (
    SELECT
        weather.timestamp,
        weather.date,
        weather.hour_of_day,
        weather.station_id,
        weather.temperature_c,
        weather.dew_point_c,
        weather.pressure_hpa,
        weather.wind_speed_ms,
        weather.heating_degree_c,
        weather.cooling_degree_c,
        weather.is_freezing,
        weather.temperature_category,
        weather.source_id,
        weather.source_uploaded_at,
        
        -- Join to date dimension to get date_id
        TO_CHAR(weather.date, 'YYYYMMDD') AS date_id,
        
        -- Join to location dimension to get location_id
        loc.location_id
    FROM deduplicated_weather weather
    LEFT JOIN {{ ref('dim_location') }} loc
        ON weather.station_id = loc.location_code
        AND loc.location_type = 'Weather Station'
        AND weather.date BETWEEN COALESCE(loc.valid_from_date, '1900-01-01') AND COALESCE(loc.valid_to_date, '9999-12-31')
)

SELECT
    -- Dimension keys
    date_id,
    hour_of_day,
    station_id,
    location_id,
    
    -- Time attributes
    timestamp,
    date,
    
    -- Weather measurements
    temperature_c,
    dew_point_c,
    pressure_hpa,
    wind_speed_ms,
    
    -- Additional weather metrics
    heating_degree_c,
    cooling_degree_c,
    
    -- Weather categories and flags
    temperature_category,
    is_freezing,
    
    -- Derived metrics
    CASE
        WHEN LAG(temperature_c) OVER (PARTITION BY station_id ORDER BY timestamp) IS NOT NULL
        THEN temperature_c - LAG(temperature_c) OVER (PARTITION BY station_id ORDER BY timestamp)
        ELSE NULL
    END AS hourly_temp_change_c,
    
    -- Calculated wind chill (feels-like temperature)
    CASE
        WHEN temperature_c < 10 AND wind_speed_ms > 1.3
        THEN 13.12 + 0.6215 * temperature_c - 11.37 * POWER(wind_speed_ms, 0.16) + 0.3965 * temperature_c * POWER(wind_speed_ms, 0.16)
        ELSE temperature_c
    END AS feels_like_temp_c,
    
    -- Source tracking
    source_id,
    source_uploaded_at,
    CURRENT_TIMESTAMP() AS loaded_at
FROM with_dimension_keys
