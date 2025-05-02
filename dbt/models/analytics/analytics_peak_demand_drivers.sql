{{
  config(
    materialized = 'table',
    schema = 'analytics'
  )
}}

WITH hourly_demand_weather AS (
    -- Join hourly demand and weather data with threshold identification
    SELECT
        d.date_id,
        d.hour_of_day,
        d.location_id,
        d.timestamp,
        d.demand_mwh,
        l.city AS location_name,
        TO_DATE(d.date_id, 'YYYYMMDD') AS date,
        DAYNAME(TO_DATE(d.date_id, 'YYYYMMDD')) AS day_name,
        dt.season,
        dt.is_weekend,
        dt.is_holiday,
        
        -- Weather station data via bridge table
        w.temperature_c,
        w.dew_point_c,
        w.pressure_hpa,
        w.wind_speed_ms,
        
        -- Calculate percentile rank of this demand
        PERCENT_RANK() OVER (
            PARTITION BY d.location_id
            ORDER BY d.demand_mwh
        ) AS demand_percentile,
        
        -- Flag peak hours (top 5% of demand)
        CASE
            WHEN PERCENT_RANK() OVER (
                PARTITION BY d.location_id
                ORDER BY d.demand_mwh
            ) >= 0.95 THEN TRUE
            ELSE FALSE
        END AS is_peak_hour,
        
        -- Flag extreme temperature hours (outside 10th-90th percentile)
        CASE
            WHEN w.temperature_c > (
                SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY temperature_c)
                FROM {{ ref('fct_weather_observations_hourly') }}
                WHERE temperature_c IS NOT NULL
            ) THEN 'EXTREME_HOT'
            WHEN w.temperature_c < (
                SELECT PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY temperature_c)
                FROM {{ ref('fct_weather_observations_hourly') }}
                WHERE temperature_c IS NOT NULL
            ) THEN 'EXTREME_COLD'
            ELSE 'NORMAL'
        END AS temperature_zone
        
    FROM {{ ref('fct_electricity_demand_hourly') }} d
    JOIN {{ ref('dim_date') }} dt ON d.date_id = dt.date_id
    JOIN {{ ref('dim_location') }} l ON d.location_id = l.location_id
    -- Join through the bridge table to map BA to weather station
    LEFT JOIN {{ ref('location_mapping_bridge') }} bridge 
        ON d.location_id = bridge.balancing_authority_id
    -- Join to weather data using the mapped weather station ID
    LEFT JOIN {{ ref('fct_weather_observations_hourly') }} w 
        ON d.date_id = w.date_id 
        AND d.hour_of_day = w.hour_of_day
        AND bridge.weather_station_id = w.location_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY d.date_id, d.hour_of_day, d.location_id ORDER BY w.timestamp) = 1
),

-- Seasonal demand patterns
seasonal_baselines AS (
    SELECT
        location_id,
        season,
        hour_of_day,
        is_weekend,
        AVG(demand_mwh) AS seasonal_baseline_mwh
    FROM hourly_demand_weather
    GROUP BY location_id, season, hour_of_day, is_weekend
),

-- Weather-demand relationship by hour
hourly_weather_impact AS (
    SELECT
        location_id,
        hour_of_day,
        season,
        CORR(temperature_c, demand_mwh) AS temp_demand_correlation,
        REGR_SLOPE(demand_mwh, temperature_c) AS temp_sensitivity_mwh_per_degree
    FROM hourly_demand_weather
    WHERE temperature_c IS NOT NULL
    GROUP BY location_id, hour_of_day, season
),

-- Enrich with baseline and sensitivity data
enriched_hourly_data AS (
    SELECT
        h.*,
        s.seasonal_baseline_mwh,
        w.temp_demand_correlation,
        w.temp_sensitivity_mwh_per_degree,
        
        -- Calculate weather contribution to demand
        CASE
            WHEN w.temp_sensitivity_mwh_per_degree IS NOT NULL AND h.temperature_c IS NOT NULL
            THEN (h.temperature_c - (
                SELECT AVG(temperature_c) 
                FROM hourly_demand_weather 
                WHERE season = h.season AND location_id = h.location_id
                AND temperature_c IS NOT NULL
            )) * w.temp_sensitivity_mwh_per_degree
            ELSE 0
        END AS weather_contribution_mwh,
        
        -- Calculate day-of-week contribution
        CASE
            WHEN h.is_weekend
            THEN h.demand_mwh - (
                SELECT AVG(demand_mwh) 
                FROM hourly_demand_weather 
                WHERE season = h.season AND hour_of_day = h.hour_of_day AND location_id = h.location_id AND NOT is_weekend
            )
            ELSE 0
        END AS weekend_contribution_mwh,
        
        -- Calculate time-of-day contribution
        h.demand_mwh - (
            SELECT AVG(demand_mwh) 
            FROM hourly_demand_weather 
            WHERE season = h.season AND location_id = h.location_id
        ) AS time_of_day_contribution_mwh
        
    FROM hourly_demand_weather h
    JOIN seasonal_baselines s 
        ON h.location_id = s.location_id 
        AND h.season = s.season 
        AND h.hour_of_day = s.hour_of_day 
        AND h.is_weekend = s.is_weekend
    LEFT JOIN hourly_weather_impact w 
        ON h.location_id = w.location_id 
        AND h.hour_of_day = w.hour_of_day 
        AND h.season = w.season
)

-- Final output focusing on peak demand factors
SELECT
    {{ dbt_utils.generate_surrogate_key(['date_id', 'hour_of_day', 'location_id']) }} AS peak_driver_id,
    date_id,
    hour_of_day,
    location_id,
    location_name,
    timestamp,
    date,
    day_name,
    season,
    is_weekend,
    is_holiday,
    
    -- Demand metrics
    demand_mwh,
    demand_percentile,
    is_peak_hour,
    seasonal_baseline_mwh,
    
    -- Weather metrics
    temperature_c,
    temperature_zone,
    temp_demand_correlation,
    
    -- Component breakdown (what's driving demand)
    weather_contribution_mwh,
    weekend_contribution_mwh,
    time_of_day_contribution_mwh,
    
    -- Percentage contribution from each factor
    CASE
        WHEN demand_mwh > 0 
        THEN (weather_contribution_mwh / NULLIF(demand_mwh, 0)) * 100
        ELSE 0
    END AS weather_contribution_percent,
    
    CASE
        WHEN demand_mwh > 0 
        THEN (weekend_contribution_mwh / NULLIF(demand_mwh, 0)) * 100
        ELSE 0
    END AS weekend_contribution_percent,
    
    CASE
        WHEN demand_mwh > 0 
        THEN (time_of_day_contribution_mwh / NULLIF(demand_mwh, 0)) * 100
        ELSE 0
    END AS time_of_day_contribution_percent,
    
    -- Unexplained remainder
    CASE
        WHEN demand_mwh > 0
        THEN demand_mwh - (weather_contribution_mwh + weekend_contribution_mwh + time_of_day_contribution_mwh)
        ELSE 0
    END AS unexplained_demand_mwh,
    
    -- Flag primary driver
    CASE
        WHEN ABS(weather_contribution_mwh) > ABS(weekend_contribution_mwh) AND 
             ABS(weather_contribution_mwh) > ABS(time_of_day_contribution_mwh)
        THEN 'WEATHER'
        WHEN ABS(weekend_contribution_mwh) > ABS(weather_contribution_mwh) AND 
             ABS(weekend_contribution_mwh) > ABS(time_of_day_contribution_mwh)
        THEN 'WEEKEND_EFFECT'
        ELSE 'TIME_OF_DAY'
    END AS primary_driver,
    
    CURRENT_TIMESTAMP() AS loaded_at
FROM enriched_hourly_data
