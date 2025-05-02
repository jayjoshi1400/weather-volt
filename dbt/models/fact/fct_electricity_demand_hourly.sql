{{
  config(
    materialized = 'incremental',
    unique_key = ['date_id', 'hour_of_day', 'location_id'],
    partition_by = {
      "field": "date",
      "data_type": "date"
    },
    on_schema_change = 'sync_all_columns',
    schema = 'fact'
  )
}}

WITH electricity_demand AS (
    SELECT
        stg_demand.timestamp,
        DATE(stg_demand.timestamp) AS date,
        stg_demand.hour_of_day,
        stg_demand.day_part,
        stg_demand.demand_mwh,
        stg_demand.is_negative_demand,
        stg_demand.balancing_authority,
        stg_demand.source_id,
        stg_demand.source_uploaded_at
    FROM {{ ref('stg_electricity_demand_hourly') }} stg_demand
    
    {% if is_incremental() %}
    -- Only process data that's newer than what's already in the fact table
    WHERE stg_demand.source_uploaded_at > (
        SELECT MAX(source_uploaded_at) FROM {{ this }}
    )
    {% endif %}
),

-- Join with dimension tables to get surrogate keys
with_dimension_keys AS (
    SELECT
        demand.timestamp,
        demand.date,
        demand.hour_of_day,
        demand.day_part,
        demand.demand_mwh,
        demand.is_negative_demand,
        demand.balancing_authority,
        demand.source_id,
        demand.source_uploaded_at,
        
        -- Join to date dimension to get date_id
        TO_CHAR(demand.date, 'YYYYMMDD') AS date_id,
        
        -- Join to location dimension to get location_id
        loc.location_id
    FROM electricity_demand demand
    LEFT JOIN {{ ref('dim_location') }} loc
        ON demand.balancing_authority = loc.location_code
        AND loc.location_type = 'Balancing Authority'
        AND demand.date BETWEEN COALESCE(loc.valid_from_date, '1900-01-01') AND COALESCE(loc.valid_to_date, '9999-12-31')
)

SELECT
    -- Dimension keys
    date_id,
    hour_of_day,
    location_id,
    
    -- Time attributes
    timestamp,
    date,
    day_part,
    
    -- Facts/measures
    demand_mwh,
    
    -- Additional derived metrics
    CASE
        WHEN LAG(demand_mwh) OVER (PARTITION BY location_id ORDER BY timestamp) IS NOT NULL
        THEN demand_mwh - LAG(demand_mwh) OVER (PARTITION BY location_id ORDER BY timestamp)
        ELSE NULL
    END AS hourly_demand_change_mwh,
    
    -- Flags
    is_negative_demand,
    
    -- Source data tracking
    source_id,
    source_uploaded_at,
    CURRENT_TIMESTAMP() AS loaded_at
FROM with_dimension_keys
