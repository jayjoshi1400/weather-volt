-- models/dimension/dim_location.sql
{{
  config(
    materialized = 'table',
    schema = 'dimension'
  )
}}

WITH generation_locations AS (
    -- Get unique locations from generation data
    SELECT DISTINCT
        location_code,
        state_name,
        'State' AS location_type,
        NULL AS county,
        NULL AS city,
        NULL AS latitude,
        NULL AS longitude
    FROM {{ ref('stg_electricity_generation_monthly') }}
),

weather_locations AS (
    -- Get unique locations from weather stations
    SELECT DISTINCT
        station_id AS location_code,
        state AS state_name,
        'Weather Station' AS location_type,
        county,
        station_name AS city,
        latitude,
        longitude
    FROM {{ ref('stg_weather_station_metadata') }}
),

balancing_authorities AS (
    -- Get unique balancing authorities
    SELECT DISTINCT
        balancing_authority AS location_code,
        'North Carolina' AS state_name,
        'Balancing Authority' AS location_type,
        NULL AS county,
        CASE
            WHEN balancing_authority = 'CPLE' THEN 'Duke Energy Progress East'
            WHEN balancing_authority = 'DUK' THEN 'Duke Energy Carolinas'
            ELSE balancing_authority
        END AS city,
        NULL AS latitude,
        NULL AS longitude
    FROM {{ ref('stg_electricity_demand_hourly') }}
),

-- Combine all locations
all_locations AS (
    SELECT * FROM generation_locations
    UNION
    SELECT * FROM weather_locations
    UNION
    SELECT * FROM balancing_authorities
),

-- Get population data, approximate - Manually defined
population_data AS (
    SELECT * FROM (
        VALUES
        ('NC', 10.7), -- NC population in millions
        ('CPLE', 1.8), -- Estimated population served in millions
        ('DUK', 2.7)  -- Estimated population served in millions
    ) AS t(location_code, population_millions)
),

enriched_locations AS (
    SELECT
        l.*,
        p.population_millions
    FROM all_locations l
    LEFT JOIN population_data p ON l.location_code = p.location_code
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['location_code', 'location_type']) }} AS location_id,
    location_code,
    state_name,
    location_type,
    county,
    city,
    latitude,
    longitude,
    population_millions,
    CASE
        WHEN location_type = 'Balancing Authority' THEN 'Regional'
        WHEN location_type = 'State' THEN 'State'
        WHEN location_type = 'Weather Station' THEN 'Local'
        ELSE 'Unknown'
    END AS geography_level,
    TRUE AS is_active, -- All locations are currently active
    CURRENT_DATE() AS valid_from_date,
    NULL AS valid_to_date, -- NULL for active records (SCD Type 2)
    CURRENT_TIMESTAMP() AS loaded_at
FROM enriched_locations
