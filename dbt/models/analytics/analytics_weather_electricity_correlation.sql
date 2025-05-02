{{
  config(
    materialized = 'table',
    schema = 'analytics'
  )
}}

WITH daily_electricity_demand AS (
    -- Aggregate hourly demand to daily level
    SELECT
        date_id,
        location_id,
        DATE(timestamp) AS date,
        SUM(demand_mwh) AS daily_demand_mwh,
        AVG(demand_mwh) AS avg_hourly_demand_mwh,
        MAX(demand_mwh) AS peak_demand_mwh
    FROM {{ ref('fct_electricity_demand_hourly') }}
    GROUP BY date_id, location_id, DATE(timestamp)
),

daily_weather_metrics AS (
    -- Aggregate hourly weather to daily level with key metrics
    SELECT
        date_id,
        station_id,
        location_id,
        date,
        AVG(temperature_c) AS avg_temp_c,
        MAX(temperature_c) AS max_temp_c,
        MIN(temperature_c) AS min_temp_c,
        MAX(temperature_c) - MIN(temperature_c) AS temp_range_c,
        SUM(heating_degree_c) AS heating_degree_day_c,
        SUM(cooling_degree_c) AS cooling_degree_day_c,
        AVG(wind_speed_ms) AS avg_wind_speed_ms,
        MAX(wind_speed_ms) AS max_wind_speed_ms,
        
        -- Count hours in specific temperature ranges
        SUM(CASE WHEN temperature_c > 26.7 THEN 1 ELSE 0 END) AS hot_hours,
        SUM(CASE WHEN temperature_c < 0 THEN 1 ELSE 0 END) AS freezing_hours
    FROM {{ ref('fct_weather_observations_hourly') }}
    GROUP BY date_id, station_id, location_id, date
),

-- Get date dimension attributes
date_attributes AS (
    SELECT
        date_id,
        is_weekend,
        is_holiday,
        season
    FROM {{ ref('dim_date') }}
),

-- Join demand and weather data using the bridge table
weather_demand_daily AS (
    SELECT
        d.date_id,
        d.date,
        d.location_id AS balancing_authority_id,
        d.daily_demand_mwh,
        d.avg_hourly_demand_mwh,
        d.peak_demand_mwh,
        
        -- Get the average weather across stations mapped to this BA
        AVG(w.avg_temp_c) AS regional_avg_temp_c,
        AVG(w.max_temp_c) AS regional_max_temp_c,
        AVG(w.min_temp_c) AS regional_min_temp_c,
        AVG(w.temp_range_c) AS regional_temp_range_c,
        AVG(w.heating_degree_day_c) AS regional_hdd_c,
        AVG(w.cooling_degree_day_c) AS regional_cdd_c,
        AVG(w.avg_wind_speed_ms) AS regional_avg_wind_ms,
        
        -- Date attributes
        da.is_weekend,
        da.is_holiday,
        da.season
    FROM daily_electricity_demand d
    -- Join to date attributes
    JOIN date_attributes da ON d.date_id = da.date_id
    -- Join through the location bridge to map BA to weather station
    LEFT JOIN {{ ref('location_mapping_bridge') }} bridge 
        ON d.location_id = bridge.balancing_authority_id
    -- Join to weather metrics using the mapped weather station ID
    LEFT JOIN daily_weather_metrics w 
        ON d.date_id = w.date_id
        AND bridge.weather_station_id = w.location_id
    GROUP BY 
        d.date_id, 
        d.date, 
        d.location_id,
        d.daily_demand_mwh,
        d.avg_hourly_demand_mwh,
        d.peak_demand_mwh,
        da.is_weekend,
        da.is_holiday,
        da.season
),

seasonal_sensitivity AS (
    SELECT
        balancing_authority_id,
        season,
        -- Correlation between temperature and demand
        CORR(regional_avg_temp_c, daily_demand_mwh) AS temp_demand_correlation,
        
        -- Linear regression coefficient (simplified)
        REGR_SLOPE(daily_demand_mwh, regional_avg_temp_c) AS temp_sensitivity_mwh_per_degree,
        
        -- Average demand at different temperature ranges
        AVG(CASE WHEN regional_avg_temp_c > 25 THEN daily_demand_mwh ELSE NULL END) AS hot_weather_avg_demand,
        AVG(CASE WHEN regional_avg_temp_c BETWEEN 15 AND 25 THEN daily_demand_mwh ELSE NULL END) AS mild_weather_avg_demand,
        AVG(CASE WHEN regional_avg_temp_c < 5 THEN daily_demand_mwh ELSE NULL END) AS cold_weather_avg_demand,
        
        -- Threshold analysis - find temperature points where demand changes significantly
        APPROX_PERCENTILE(regional_avg_temp_c, 0.9) AS high_temp_threshold,
        APPROX_PERCENTILE(regional_avg_temp_c, 0.1) AS low_temp_threshold
    FROM weather_demand_daily
    GROUP BY balancing_authority_id, season
)

-- Final output combining daily data with seasonal sensitivity
SELECT
    wd.date_id,
    wd.date,
    wd.balancing_authority_id,
    b.city AS balancing_authority_name,
    wd.daily_demand_mwh,
    wd.peak_demand_mwh,
    wd.regional_avg_temp_c,
    wd.regional_max_temp_c,
    wd.regional_min_temp_c,
    wd.regional_hdd_c,
    wd.regional_cdd_c,
    wd.season,
    wd.is_weekend,
    wd.is_holiday,
    
    -- Include seasonal sensitivity metrics
    ss.temp_demand_correlation,
    ss.temp_sensitivity_mwh_per_degree,
    
    -- Calculate expected demand based on temperature
    wd.regional_avg_temp_c * ss.temp_sensitivity_mwh_per_degree + 
        (SELECT AVG(daily_demand_mwh) FROM weather_demand_daily WHERE balancing_authority_id = wd.balancing_authority_id) 
        AS temperature_expected_demand,
    
    -- Demand anomaly (actual vs temperature-expected)
    wd.daily_demand_mwh - (
        wd.regional_avg_temp_c * ss.temp_sensitivity_mwh_per_degree + 
        (SELECT AVG(daily_demand_mwh) FROM weather_demand_daily WHERE balancing_authority_id = wd.balancing_authority_id)
    ) AS demand_anomaly_mwh,
    
    -- Weather zone flags
    CASE
        WHEN wd.regional_avg_temp_c > ss.high_temp_threshold THEN 'HIGH_TEMP_ZONE'
        WHEN wd.regional_avg_temp_c < ss.low_temp_threshold THEN 'LOW_TEMP_ZONE'
        ELSE 'NORMAL_TEMP_ZONE'
    END AS temperature_zone,
    
    -- Calendar impact estimates
    CASE
        WHEN wd.is_weekend THEN 
            wd.daily_demand_mwh / NULLIF(
                (SELECT AVG(daily_demand_mwh) 
                 FROM weather_demand_daily 
                 WHERE balancing_authority_id = wd.balancing_authority_id AND NOT is_weekend)
            , 0)
        ELSE 1
    END AS weekend_demand_ratio,
    
    CURRENT_TIMESTAMP() AS loaded_at
FROM weather_demand_daily wd
JOIN seasonal_sensitivity ss ON wd.balancing_authority_id = ss.balancing_authority_id AND wd.season = ss.season
JOIN {{ ref('dim_location') }} b ON wd.balancing_authority_id = b.location_id
