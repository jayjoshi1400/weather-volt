{{
  config(
    materialized = 'view',
    schema = 'staging'
  )
}}

WITH source_data AS (
    SELECT
        id AS source_id,
        file_name,
        uploaded_at,
        data_date,
        raw_data
    FROM {{ source('raw', 'EIA_ELECTRICITY_GENERATION_JSON') }}
),

flattened_data AS (
    SELECT
        source_id,
        uploaded_at,
        file_name,
        f.value AS generation_data
    FROM source_data,
    LATERAL FLATTEN(input => raw_data:response.data) f
),

parsed_data AS (
    SELECT
        source_id,
        uploaded_at,
        file_name,
        generation_data:period::STRING AS period,
        generation_data:location::STRING AS location_code,
        generation_data:stateDescription::STRING AS state_name,
        generation_data:fueltypeid::STRING AS fuel_type_id,
        generation_data:fuelTypeDescription::STRING AS fuel_type_name,
        generation_data:sectorid::STRING AS sector_id,
        generation_data:sectorDescription::STRING AS sector_name,
        generation_data:generation::FLOAT AS generation_mwh,
        generation_data:"generation-units"::STRING AS generation_units,
        TO_DATE(generation_data:period::STRING, 'YYYY-MM') AS generation_date,
        EXTRACT(YEAR FROM generation_date) AS year,
        EXTRACT(MONTH FROM generation_date) AS month
    FROM flattened_data
),

with_grain_id AS (
    SELECT
        *,
        {{ dbt_utils.generate_surrogate_key([
            'period', 
            'location_code', 
            'fuel_type_id', 
            'sector_id'
        ]) }} AS grain_id
    FROM parsed_data
),

marked_records AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY grain_id 
            ORDER BY uploaded_at DESC
        ) AS row_num
    FROM with_grain_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'period', 
        'location_code', 
        'fuel_type_id', 
        'sector_id'
    ]) }} AS generation_id,
    period,
    location_code,
    state_name,
    fuel_type_id,
    fuel_type_name,
    sector_id,
    sector_name,
    -- Convert from "thousand megawatthours" to megawatthours
    CASE 
        WHEN generation_units = 'thousand megawatthours' AND generation_mwh IS NOT NULL 
        THEN generation_mwh * 1000
        ELSE generation_mwh
    END AS generation_mwh,
    generation_units,
    -- Calculate renewable flag
    CASE
        WHEN fuel_type_id IN ('SUN', 'SPV', 'REN', 'WND', 'HYC', 'DPV') THEN TRUE
        ELSE FALSE
    END AS is_renewable,
    -- Calculate fossil fuel flag
    CASE
        WHEN fuel_type_id IN ('COW', 'NG', 'NGO', 'PET', 'PEL') THEN TRUE
        ELSE FALSE
    END AS is_fossil_fuel,
    generation_date,
    year,
    month,
    grain_id,
    uploaded_at AS source_timestamp,
    source_id,
    file_name AS source_file,
    CURRENT_TIMESTAMP() AS loaded_at
FROM marked_records
WHERE row_num = 1
