{{
  config(
    materialized = 'table',
    schema = 'dimension'
  )
}}

WITH energy_sources AS (
    SELECT DISTINCT
        fuel_type_id,
        fuel_type_name,
        is_renewable,
        is_fossil_fuel
    FROM {{ ref('stg_electricity_generation_monthly') }}
),

sources_with_attributes AS (
    SELECT
        fuel_type_id,
        fuel_type_name,
        is_renewable,
        is_fossil_fuel,
        CASE
            WHEN fuel_type_id = 'COW' THEN 'Coal'
            WHEN fuel_type_id = 'NG' THEN 'Natural Gas'
            WHEN fuel_type_id = 'NGO' THEN 'Natural Gas & Other'
            WHEN fuel_type_id = 'NUC' THEN 'Nuclear'
            WHEN fuel_type_id = 'PET' THEN 'Petroleum'
            WHEN fuel_type_id = 'PEL' THEN 'Petroleum Liquids'
            WHEN fuel_type_id = 'HYC' THEN 'Hydroelectric'
            WHEN fuel_type_id = 'WWW' THEN 'Wood & Waste'
            WHEN fuel_type_id = 'SUN' THEN 'Solar'
            WHEN fuel_type_id = 'SPV' THEN 'Solar Photovoltaic'
            WHEN fuel_type_id = 'WND' THEN 'Wind'
            WHEN fuel_type_id = 'GEO' THEN 'Geothermal'
            WHEN fuel_type_id = 'OTH' THEN 'Other'
            WHEN fuel_type_id = 'REN' THEN 'Renewables'
            WHEN fuel_type_id = 'DPV' THEN 'Distributed Solar'
            WHEN fuel_type_id = 'ALL' THEN 'All Sources'
            ELSE fuel_type_name
        END AS source_category,
        
        -- Carbon intensity (kg CO2 per MWh) - approximate values, just to have something initially. Can be updated later
        CASE
            WHEN fuel_type_id = 'COW' THEN 1000  -- Coal
            WHEN fuel_type_id IN ('NG', 'NGO') THEN 450  -- Natural Gas
            WHEN fuel_type_id IN ('PET', 'PEL') THEN 800  -- Petroleum
            WHEN fuel_type_id = 'NUC' THEN 10  -- Nuclear
            WHEN fuel_type_id IN ('HYC', 'SUN', 'SPV', 'WND', 'GEO') THEN 0  -- Renewables
            ELSE NULL
        END AS carbon_intensity_kg_per_mwh,
        
        -- Dispatchability (can it be turned on/off on demand)
        CASE
            WHEN fuel_type_id IN ('COW', 'NG', 'NGO', 'PET', 'PEL') THEN TRUE  -- Fossil fuels
            WHEN fuel_type_id = 'NUC' THEN FALSE  -- Nuclear 
            WHEN fuel_type_id = 'HYC' THEN TRUE  -- Hydro 
            WHEN fuel_type_id IN ('SUN', 'SPV', 'WND') THEN FALSE  -- Solar/Wind (weather dependent)
            ELSE NULL
        END AS is_dispatchable
    FROM energy_sources
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['fuel_type_id']) }} AS source_id,
    ROW_NUMBER() OVER (ORDER BY fuel_type_id) AS source_key, -- Numeric key
    fuel_type_id,
    fuel_type_name,
    source_category,
    
    CASE
        WHEN is_renewable THEN 'Renewable'
        WHEN is_fossil_fuel THEN 'Fossil Fuel'
        WHEN fuel_type_id = 'NUC' THEN 'Nuclear'
        WHEN fuel_type_id = 'ALL' THEN 'All Sources'
        ELSE 'Other'
    END AS primary_category,
    
    is_renewable,
    is_fossil_fuel,
    CASE WHEN fuel_type_id = 'NUC' THEN TRUE ELSE FALSE END AS is_nuclear,
    
    carbon_intensity_kg_per_mwh,
    is_dispatchable,
    
    CURRENT_TIMESTAMP() AS loaded_at
FROM sources_with_attributes
