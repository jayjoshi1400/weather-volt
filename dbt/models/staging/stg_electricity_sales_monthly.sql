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
    FROM {{ source('raw', 'EIA_RETAIL_SALES_JSON') }}
),

flattened_data AS (
    SELECT
        source_id,
        uploaded_at,
        file_name,
        f.value AS sales_data
    FROM source_data,
    LATERAL FLATTEN(input => raw_data:response.data) f
),

parsed_data AS (
    SELECT
        source_id,
        uploaded_at,
        file_name,
        sales_data:period::STRING AS period,
        sales_data:stateid::STRING AS state_id,
        sales_data:stateDescription::STRING AS state_name,
        sales_data:sectorid::STRING AS sector_id,
        sales_data:sectorName::STRING AS sector_name,    
        sales_data:sales::FLOAT AS sales_mwh,            
        sales_data:revenue::FLOAT AS revenue_usd,
        sales_data:price::FLOAT AS price_cents_per_kwh,
        sales_data:customers::INTEGER AS customer_count,
        TO_DATE(sales_data:period::STRING, 'YYYY-MM') AS sales_date,
        EXTRACT(YEAR FROM sales_date) AS year,
        EXTRACT(MONTH FROM sales_date) AS month
    FROM flattened_data
    WHERE sales_data:stateid::STRING = 'NC' 
)
,

with_grain_id AS (
    SELECT
        *,
        {{ dbt_utils.generate_surrogate_key([
            'period', 
            'state_id', 
            'sector_id', 
            'sales_mwh', 
            'revenue_usd', 
            'price_cents_per_kwh', 
            'customer_count'
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
        'state_id', 
        'sector_id'
    ]) }} AS sales_id,
    period,
    state_id,
    state_name,
    sector_id,
    sector_name,
    sales_mwh,
    revenue_usd,
    price_cents_per_kwh,
    customer_count,
    sales_date,
    year,
    month,
    grain_id,
    uploaded_at AS source_timestamp,
    source_id,
    file_name AS source_file,
    CURRENT_TIMESTAMP() AS loaded_at
FROM marked_records
WHERE row_num = 1  
