-- models/analytics_special/state_tracking/state_tracking_grid_demand_level.sql

{{
  config(
    materialized = 'incremental',
    unique_key = 'state_change_id',
    schema = 'analytics',
    on_schema_change = 'sync_all_columns'
  )
}}

WITH hourly_demand_data AS (
    -- Get hourly demand data with state classification
    SELECT
        f.location_id AS balancing_authority_id,
        f.timestamp,
        f.date_id,
        f.hour_of_day,
        f.demand_mwh,
        d.date,
        d.day_name,
        d.is_weekend,
        d.is_holiday,
        
        -- Define demand states
        CASE
            WHEN f.demand_mwh > (
                SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY demand_mwh)
                FROM {{ ref('fct_electricity_demand_hourly') }}
                WHERE location_id = f.location_id
            ) THEN 'CRITICAL'
            WHEN f.demand_mwh > (
                SELECT PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY demand_mwh)
                FROM {{ ref('fct_electricity_demand_hourly') }}
                WHERE location_id = f.location_id
            ) THEN 'PEAK'
            WHEN f.demand_mwh > (
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY demand_mwh)
                FROM {{ ref('fct_electricity_demand_hourly') }}
                WHERE location_id = f.location_id
            ) THEN 'HIGH'
            WHEN f.demand_mwh > (
                SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY demand_mwh)
                FROM {{ ref('fct_electricity_demand_hourly') }}
                WHERE location_id = f.location_id
            ) THEN 'NORMAL'
            ELSE 'LOW'
        END AS demand_state
        
    FROM {{ ref('fct_electricity_demand_hourly') }} f
    JOIN {{ ref('dim_date') }} d ON f.date_id = d.date_id
    
    {% if is_incremental() %}
    -- Only process data that's newer than what we've already processed
    -- Add some overlap to ensure we catch transitions that span processed periods
    WHERE f.timestamp > (SELECT DATEADD(day, -1, MAX(state_change_timestamp)) FROM {{ this }})
    {% endif %}
),

-- Add weather conditions at the time of observation using the bridge table
with_weather_conditions AS (
    SELECT
        d.*,
        -- Get weather observation from mapped station
        w.temperature_c,
        w.pressure_hpa,
        w.wind_speed_ms
    FROM hourly_demand_data d
    -- Join through the location bridge to map BA to weather station
    LEFT JOIN {{ ref('location_mapping_bridge') }} bridge 
        ON d.balancing_authority_id = bridge.balancing_authority_id
    -- Join to weather data using the mapped weather station ID
    LEFT JOIN {{ ref('fct_weather_observations_hourly') }} w 
        ON d.date_id = w.date_id 
        AND d.hour_of_day = w.hour_of_day
        AND bridge.weather_station_id = w.location_id
    -- If multiple observations, take the closest one
    QUALIFY ROW_NUMBER() OVER (PARTITION BY d.balancing_authority_id, d.timestamp ORDER BY w.timestamp) = 1
),

-- Identify state changes by comparing to previous hour
with_state_changes AS (
    SELECT
        *,
        LAG(demand_state) OVER (
            PARTITION BY balancing_authority_id 
            ORDER BY timestamp
        ) AS previous_demand_state,
        
        LAG(timestamp) OVER (
            PARTITION BY balancing_authority_id 
            ORDER BY timestamp
        ) AS previous_timestamp,
        
        -- Only mark a row if state has changed
        CASE
            WHEN demand_state != LAG(demand_state) OVER (
                PARTITION BY balancing_authority_id 
                ORDER BY timestamp
            ) OR LAG(demand_state) OVER (
                PARTITION BY balancing_authority_id 
                ORDER BY timestamp
            ) IS NULL
            THEN 1
            ELSE 0
        END AS is_state_change
    FROM with_weather_conditions
),

-- Filter to just the state change events and calculate duration
state_change_events AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['balancing_authority_id', 'timestamp']) }} AS state_change_id,
        balancing_authority_id,
        previous_demand_state AS from_state,
        demand_state AS to_state,
        timestamp AS state_change_timestamp,
        date_id,
        
        -- Calculate duration of previous state
        CASE
            WHEN previous_timestamp IS NOT NULL
            THEN DATEDIFF('minute', previous_timestamp, timestamp)
            ELSE NULL
        END AS previous_state_duration_minutes,
        
        -- Determine what triggered the change
        CASE
            -- Morning ramp up
            WHEN hour_of_day BETWEEN 5 AND 9 
              AND (to_state = 'HIGH' OR to_state = 'PEAK') 
              AND (from_state = 'LOW' OR from_state = 'NORMAL')
            THEN 'MORNING_RAMP'
            
            -- Evening peak
            WHEN hour_of_day BETWEEN 17 AND 21 
              AND (to_state = 'PEAK' OR to_state = 'CRITICAL')
            THEN 'EVENING_PEAK'
            
            -- Weekend drop
            WHEN is_weekend AND from_state IN ('NORMAL', 'HIGH', 'PEAK') 
              AND to_state IN ('LOW', 'NORMAL')
            THEN 'WEEKEND_REDUCTION'
            
            -- Weather driven
            WHEN temperature_c IS NOT NULL AND (
              (temperature_c > 30 AND to_state IN ('PEAK', 'CRITICAL')) OR
              (temperature_c < 0 AND to_state IN ('PEAK', 'CRITICAL'))
            )
            THEN 'EXTREME_TEMPERATURE'
            
            -- Default
            ELSE 'NORMAL_FLUCTUATION'
        END AS change_trigger,
        
        -- Contextual measurements at change point
        demand_mwh AS demand_at_change_mwh,
        temperature_c,
        pressure_hpa,
        wind_speed_ms,
        
        -- Additional context
        day_name,
        is_weekend,
        is_holiday
    FROM with_state_changes
    WHERE is_state_change = 1
    AND previous_demand_state IS NOT NULL  -- Skip the very first observation with no previous state
)

-- Final output
SELECT * FROM state_change_events

{% if is_incremental() %}
-- For incremental loads, exclude any state changes we've already captured
WHERE state_change_id NOT IN (SELECT state_change_id FROM {{ this }})
{% endif %}
