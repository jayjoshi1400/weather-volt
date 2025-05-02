-- models/staging/stg_weather_station_metadata.sql
{{
  config(
    materialized='view',
    schema='STAGING'
  )
}}

WITH all_station_ids AS (
    -- Get every unique station ID from the data
    SELECT DISTINCT
        station_id
    FROM {{ source('raw', 'NOAA_ISD_WEATHER_JSON') }}
),

station_year_ranges AS (
    -- Calculate the year ranges for each station
    SELECT 
        station_id,
        MIN(year) AS first_year,
        MAX(year) AS last_year,
        MAX(uploaded_at) AS latest_upload
    FROM {{ source('raw', 'NOAA_ISD_WEATHER_JSON') }}
    GROUP BY station_id
),

-- Expanded lookup table with known station coordinates
station_lookup AS (
    SELECT * FROM (
        VALUES
        ('723060-13722', 35.8801, -78.7880, 416, 'Raleigh-Durham International Airport'), 
        ('723140-13881', 35.2144, -80.9494, 748, 'Charlotte Douglas International Airport'), 
        ('723170-93807', 36.0977, -79.9437, 925, 'Piedmont Triad International Airport'), 
        ('723150-03812', 35.4361, -82.5375, 2165, 'Asheville Regional Airport'), 
        ('723013-13748', 34.2681, -77.9053, 32, 'Wilmington International Airport'),
        ('723086-93759', 36.2605, -76.1747, 12, 'Elizabeth City CGAS Airport'),
        ('722201-53871', 35.7111, -82.4878, 2170, 'Asheville Mountain Research Station')
        -- Add more stations as needed
    ) AS t(station_id, latitude, longitude, elevation, station_name)
),

-- Join all stations with lookup data
enriched_stations AS (
    SELECT
        a.station_id,
        r.first_year,
        r.last_year,
        r.latest_upload,
        l.latitude,
        l.longitude,
        l.elevation,
        l.station_name AS known_station_name
    FROM all_station_ids a
    LEFT JOIN station_year_ranges r ON a.station_id = r.station_id
    LEFT JOIN station_lookup l ON a.station_id = l.station_id
),

-- Add geographical context by determining North Carolina counties
station_counties AS (
    SELECT
        s.*,
        CASE
            WHEN latitude IS NULL THEN 'Unknown County'
            WHEN latitude BETWEEN 35.43 AND 36.52 AND longitude BETWEEN -80.87 AND -80.02 THEN 'Forsyth'
            WHEN latitude BETWEEN 35.11 AND 36.08 AND longitude BETWEEN -81.20 AND -80.56 THEN 'Mecklenburg'
            WHEN latitude BETWEEN 35.72 AND 36.35 AND longitude BETWEEN -79.22 AND -78.28 THEN 'Wake'
            WHEN latitude BETWEEN 34.09 AND 34.48 AND longitude BETWEEN -78.10 AND -77.70 THEN 'Brunswick'
            WHEN latitude BETWEEN 35.43 AND 36.07 AND longitude BETWEEN -83.19 AND -82.17 THEN 'Buncombe'
            ELSE 'Other NC County'
        END AS county
    FROM enriched_stations s
)

SELECT
    station_id,
    {{ dbt_utils.generate_surrogate_key(['station_id']) }} AS station_key,
    SPLIT_PART(station_id, '-', 1) AS usaf_id,
    SPLIT_PART(station_id, '-', 2) AS wban_id,
    
    -- Station location information (may be NULL)
    latitude,
    longitude,
    elevation AS elevation_m,
    county,
    'North Carolina' AS state,
    'USA' AS country,
    
    -- Active period information
    first_year AS data_from_year,
    last_year AS data_to_year,
    
    -- Station name with fallback for unknown stations
    CASE 
        WHEN known_station_name IS NOT NULL THEN known_station_name
        ELSE 'Weather Station ' || station_id
    END AS station_name,
    
    -- Additional metadata
    CASE WHEN known_station_name LIKE '%Airport%' THEN TRUE ELSE FALSE END AS is_airport,
    
    -- Data quality flags
    CASE WHEN latitude IS NULL OR longitude IS NULL THEN FALSE ELSE TRUE END AS has_coordinates,
    
    CURRENT_TIMESTAMP() AS loaded_at

FROM station_counties
