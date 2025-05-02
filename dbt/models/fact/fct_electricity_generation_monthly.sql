{{
  config(
    materialized = 'incremental',
    unique_key = ['date_id', 'location_id', 'energy_source_id', 'sector_id'],
    partition_by = {
      "field": "generation_date",
      "data_type": "date",
      "granularity": "month"
    },
    on_schema_change = 'sync_all_columns',
    schema = 'fact'
  )
}}

WITH generation_data AS (
    SELECT
        stg_gen.period,
        stg_gen.location_code,
        stg_gen.state_name,
        stg_gen.fuel_type_id,
        stg_gen.fuel_type_name,
        stg_gen.sector_id,
        stg_gen.sector_name,
        stg_gen.generation_mwh,
        stg_gen.is_renewable,
        stg_gen.is_fossil_fuel,
        stg_gen.generation_date,
        stg_gen.year,
        stg_gen.month,
        stg_gen.source_id AS record_source_id,
        stg_gen.source_timestamp,
        TO_CHAR(DATE_TRUNC('MONTH', stg_gen.generation_date), 'YYYYMMDD') AS date_id
    FROM {{ ref('stg_electricity_generation_monthly') }} stg_gen
    
    {% if is_incremental() %}
    WHERE stg_gen.source_timestamp > (
        SELECT MAX(source_uploaded_at) FROM {{ this }}
    )
    {% endif %}
),

-- Get dimension mappings first, completely separate from the filtering logic
dim_energy_sources AS (
    SELECT 
        fuel_type_id,
        source_id AS energy_source_id
    FROM {{ ref('dim_energy_source') }}
),

dim_locations AS (
    SELECT 
        location_code,
        location_id
    FROM {{ ref('dim_location') }}
    WHERE location_type = 'State'
),

-- Join source data with dimensions
joined_data AS (
    SELECT
        g.*,
        l.location_id,
        e.energy_source_id
    FROM generation_data g
    LEFT JOIN dim_locations l ON g.location_code = l.location_code
    LEFT JOIN dim_energy_sources e ON g.fuel_type_id = e.fuel_type_id
),

{% if is_incremental() %}
-- Find existing records to filter them out
existing_records AS (
    SELECT 
        date_id,
        sector_id,
        location_id,
        energy_source_id
    FROM {{ this }}
),

-- Filter out existing records
filtered_generation_data AS (
    SELECT g.*
    FROM joined_data g
    LEFT JOIN existing_records e
        ON g.date_id = e.date_id
        AND g.sector_id = e.sector_id
        AND g.location_id = e.location_id
        AND g.energy_source_id = e.energy_source_id
    WHERE e.date_id IS NULL
),
{% else %}
filtered_generation_data AS (
    SELECT * FROM joined_data
),
{% endif %}

deduplicated_generation AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    period, 
                    location_code, 
                    fuel_type_id, 
                    sector_id
                ORDER BY source_timestamp DESC, record_source_id DESC
            ) as row_num
        FROM filtered_generation_data
    )
    WHERE row_num = 1
),

with_percentage AS (
    SELECT
        *,
        SUM(generation_mwh) OVER(PARTITION BY date_id, location_id) AS total_generation_mwh,
        CASE 
            WHEN SUM(generation_mwh) OVER(PARTITION BY date_id, location_id) > 0
            THEN (generation_mwh / SUM(generation_mwh) OVER(PARTITION BY date_id, location_id)) * 100
            ELSE 0
        END AS percentage_of_total
    FROM deduplicated_generation
)

SELECT
    date_id,
    location_id,
    energy_source_id,
    sector_id, 
    period,
    generation_date,
    year,
    month,
    
    sector_name,
    
    generation_mwh,
    percentage_of_total,
    
    CASE
        WHEN LAG(generation_mwh) OVER (PARTITION BY location_id, energy_source_id, sector_id ORDER BY generation_date) IS NOT NULL
        THEN generation_mwh - LAG(generation_mwh) OVER (PARTITION BY location_id, energy_source_id, sector_id ORDER BY generation_date)
        ELSE NULL
    END AS monthly_generation_change_mwh,
    
    is_renewable,
    is_fossil_fuel,
    
    record_source_id AS source_record_id,
    source_timestamp AS source_uploaded_at,
    CURRENT_TIMESTAMP() AS loaded_at
FROM with_percentage
