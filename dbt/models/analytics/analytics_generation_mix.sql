-- models/analytics/analytics_generation_mix.sql

{{
  config(
    materialized = 'table',
    schema = 'analytics'
  )
}}

WITH monthly_generation AS (
    SELECT
        f.date_id,
        f.location_id,
        f.energy_source_id,
        d.year,
        d.month_number AS month,
        d.month_name,
        d.quarter,
        d.season,
        es.fuel_type_id,
        es.fuel_type_name,
        es.primary_category,
        es.is_renewable,
        es.is_fossil_fuel,
        es.is_nuclear,
        f.generation_mwh,
        f.percentage_of_total
    FROM {{ ref('fct_electricity_generation_monthly') }} f
    JOIN {{ ref('dim_date') }} d ON f.date_id = d.date_id
    JOIN {{ ref('dim_energy_source') }} es ON f.energy_source_id = es.source_id
),

-- Calculate renewable percentage by month and location
renewable_percentage AS (
    SELECT
        date_id,
        location_id,
        year,
        month,
        100.0 * SUM(CASE WHEN is_renewable THEN generation_mwh ELSE 0 END) / 
            NULLIF(SUM(generation_mwh), 0) AS renewable_percentage
    FROM monthly_generation
    GROUP BY date_id, location_id, year, month
),

-- Calculate year-over-year changes in mix
yoy_changes AS (
    SELECT
        current_year.date_id,
        current_year.location_id,
        current_year.energy_source_id,
        current_year.fuel_type_id,
        current_year.generation_mwh,
        current_year.percentage_of_total,
        previous_year.generation_mwh AS previous_year_generation_mwh,
        previous_year.percentage_of_total AS previous_year_percentage,
        current_year.generation_mwh - COALESCE(previous_year.generation_mwh, 0) AS yoy_change_mwh,
        current_year.percentage_of_total - COALESCE(previous_year.percentage_of_total, 0) AS yoy_change_percentage
    FROM monthly_generation current_year
    LEFT JOIN monthly_generation previous_year ON
        current_year.location_id = previous_year.location_id AND
        current_year.energy_source_id = previous_year.energy_source_id AND
        current_year.month = previous_year.month AND
        current_year.year = previous_year.year + 1
),

-- Find dominant source each month
dominant_source AS (
    SELECT
        date_id,
        location_id,
        FIRST_VALUE(fuel_type_name) OVER (
            PARTITION BY date_id, location_id
            ORDER BY generation_mwh DESC
        ) AS dominant_source_name,
        FIRST_VALUE(fuel_type_id) OVER (
            PARTITION BY date_id, location_id
            ORDER BY generation_mwh DESC
        ) AS dominant_source_id,
        FIRST_VALUE(primary_category) OVER (
            PARTITION BY date_id, location_id
            ORDER BY generation_mwh DESC
        ) AS dominant_category
    FROM monthly_generation
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date_id, location_id ORDER BY generation_mwh DESC) = 1
)

-- Final output
SELECT
    g.date_id,
    g.location_id,
    g.energy_source_id,
    g.year,
    g.month,
    g.month_name,
    g.quarter,
    g.season,
    g.fuel_type_id,
    g.fuel_type_name,
    g.primary_category,
    g.is_renewable,
    g.is_fossil_fuel,
    g.is_nuclear,
    g.generation_mwh,
    g.percentage_of_total,
    
    -- Include renewable percentage
    r.renewable_percentage,
    
    -- Year over year changes
    y.previous_year_generation_mwh,
    y.previous_year_percentage,
    y.yoy_change_mwh,
    y.yoy_change_percentage,
    
    -- Dominant source flags
    CASE WHEN g.fuel_type_id = d.dominant_source_id THEN TRUE ELSE FALSE END AS is_dominant_source,
    d.dominant_source_name,
    d.dominant_source_id,
    d.dominant_category,
    
    -- Carbon emissions estimation (very rough approximation)
    CASE
        WHEN g.fuel_type_id = 'COW' THEN g.generation_mwh * 0.9 -- ton CO2 per MWh
        WHEN g.fuel_type_id IN ('NG', 'NGO') THEN g.generation_mwh * 0.4
        WHEN g.fuel_type_id IN ('PET', 'PEL') THEN g.generation_mwh * 0.75
        ELSE 0
    END AS estimated_co2_emissions_tons,
    
    CURRENT_TIMESTAMP() AS loaded_at
FROM monthly_generation g
JOIN renewable_percentage r ON g.date_id = r.date_id AND g.location_id = r.location_id
JOIN yoy_changes y ON g.date_id = y.date_id AND g.energy_source_id = y.energy_source_id AND g.location_id = y.location_id
JOIN dominant_source d ON g.date_id = d.date_id AND g.location_id = d.location_id
