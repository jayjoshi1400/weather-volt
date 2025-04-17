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
        data_date,
        balancing_authority,
        raw_data
    FROM {{ source('raw', 'EIA_HOURLY_DEMAND_JSON') }}
),

flattened_data AS (
    SELECT
        source_id,
        file_name,
        uploaded_at,
        data_date,
        balancing_authority,
        f.value AS data_point
    FROM source_data,
    LATERAL FLATTEN(input => raw_data:response.data) f
),

deduplicated_data AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY balancing_authority, data_point:period::TIMESTAMP
            ORDER BY uploaded_at DESC, source_id DESC
        ) as row_num
    FROM flattened_data
    WHERE data_point:period::TIMESTAMP IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['balancing_authority', 'data_point:period::TIMESTAMP']) }} 
        AS demand_id,
    
    balancing_authority,
    
    data_point:period::TIMESTAMP AS timestamp,
    DATE(data_point:period::TIMESTAMP) AS date,
    EXTRACT(HOUR FROM data_point:period::TIMESTAMP) AS hour_of_day,
    CASE 
        WHEN EXTRACT(HOUR FROM data_point:period::TIMESTAMP) BETWEEN 6 AND 9 THEN 'MORNING_PEAK'
        WHEN EXTRACT(HOUR FROM data_point:period::TIMESTAMP) BETWEEN 17 AND 21 THEN 'EVENING_PEAK'
        WHEN EXTRACT(HOUR FROM data_point:period::TIMESTAMP) BETWEEN 10 AND 16 THEN 'MIDDAY'
        ELSE 'OFF_PEAK' 
    END AS day_part,
    
    data_point:value::FLOAT AS demand_mwh,
    data_point['value-units']::STRING AS units,
    
    CASE 
        WHEN data_point:value::FLOAT < 0 THEN TRUE 
        ELSE FALSE 
    END AS is_negative_demand,
    
    source_id,
    file_name,
    data_date,
    uploaded_at AS source_uploaded_at,
    CURRENT_TIMESTAMP() AS loaded_at
    
FROM deduplicated_data
WHERE row_num = 1
