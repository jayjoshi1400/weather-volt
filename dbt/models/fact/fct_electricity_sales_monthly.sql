{{
  config(
    materialized = 'incremental',
    unique_key = ['date_id', 'location_id', 'sector_id'],  
    partition_by = {
      "field": "sales_date",
      "data_type": "date",
      "granularity": "month"
    },
    on_schema_change = 'sync_all_columns',
    schema = 'fact'
  )
}}

WITH sales_data AS (
    SELECT
        stg_sales.period,
        stg_sales.state_id,
        stg_sales.state_name,
        stg_sales.sector_id,
        stg_sales.sector_name,
        stg_sales.sales_mwh,
        stg_sales.revenue_usd,
        stg_sales.price_cents_per_kwh,
        stg_sales.customer_count,
        stg_sales.sales_date,
        stg_sales.year,
        stg_sales.month,
        stg_sales.source_id,
        stg_sales.source_timestamp,
        TO_CHAR(DATE_TRUNC('MONTH', stg_sales.sales_date), 'YYYYMMDD') AS date_id
    FROM {{ ref('stg_electricity_sales_monthly') }} stg_sales
    
    {% if is_incremental() %}
    WHERE stg_sales.source_timestamp > (
        SELECT MAX(source_uploaded_at) FROM {{ this }}
    )
    {% endif %}
),

-- Pre-fetch dimension data
dim_locations AS (
    SELECT 
        location_code,
        location_id,
        valid_from_date,
        valid_to_date
    FROM {{ ref('dim_location') }}
    WHERE location_type = 'State'
),

-- Join source data with dimensions first
joined_data AS (
    SELECT
        s.*,
        l.location_id
    FROM sales_data s
    LEFT JOIN dim_locations l 
        ON s.state_id = l.location_code
        AND s.sales_date BETWEEN COALESCE(l.valid_from_date, '1900-01-01') AND COALESCE(l.valid_to_date, '9999-12-31')
),

{% if is_incremental() %}
-- Find existing records to filter them out
existing_records AS (
    SELECT 
        date_id,
        sector_id,
        location_id
    FROM {{ this }}
),

-- Filter out existing records
filtered_sales_data AS (
    SELECT j.*
    FROM joined_data j
    LEFT JOIN existing_records e
        ON j.date_id = e.date_id
        AND j.sector_id = e.sector_id
        AND j.location_id = e.location_id
    WHERE e.date_id IS NULL
),
{% else %}
filtered_sales_data AS (
    SELECT * FROM joined_data
),
{% endif %}

-- Deduplicate the sales data to ensure uniqueness
deduplicated_sales AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    period, 
                    state_id, 
                    sector_id
                ORDER BY source_timestamp DESC, source_id DESC
            ) as row_num
        FROM filtered_sales_data
    )
    WHERE row_num = 1
)

SELECT
    -- Dimension keys
    date_id,
    location_id,
    sector_id,
    
    -- Time attributes
    period,
    sales_date,
    year,
    month,
    
    -- Facts/measures
    sales_mwh,
    revenue_usd,
    price_cents_per_kwh,
    customer_count,
    
    -- Calculated metrics (with zero-division protection)
    CASE
        WHEN customer_count IS NULL OR customer_count = 0 THEN NULL
        ELSE sales_mwh / customer_count
    END AS avg_usage_per_customer_mwh,
    
    CASE
        WHEN sales_mwh IS NULL OR sales_mwh = 0 THEN NULL
        ELSE revenue_usd / sales_mwh
    END AS revenue_per_mwh_usd,
    
    -- Year-over-year changes (with zero-division protection)
    CASE
        WHEN LAG(sales_mwh, 12) OVER (PARTITION BY sector_id, location_id ORDER BY sales_date) IS NULL 
          OR LAG(sales_mwh, 12) OVER (PARTITION BY sector_id, location_id ORDER BY sales_date) = 0
        THEN NULL
        ELSE (sales_mwh - LAG(sales_mwh, 12) OVER (PARTITION BY sector_id, location_id ORDER BY sales_date)) / 
             LAG(sales_mwh, 12) OVER (PARTITION BY sector_id, location_id ORDER BY sales_date) * 100
    END AS yoy_sales_change_percent,
    
    -- Month-over-month changes (with zero-division protection)
    CASE
        WHEN LAG(sales_mwh) OVER (PARTITION BY sector_id, location_id ORDER BY sales_date) IS NULL
          OR LAG(sales_mwh) OVER (PARTITION BY sector_id, location_id ORDER BY sales_date) = 0
        THEN NULL
        ELSE (sales_mwh - LAG(sales_mwh) OVER (PARTITION BY sector_id, location_id ORDER BY sales_date)) / 
             LAG(sales_mwh) OVER (PARTITION BY sector_id, location_id ORDER BY sales_date) * 100
    END AS mom_sales_change_percent,
    
    -- Source tracking
    source_id AS source_record_id,
    source_timestamp AS source_uploaded_at,
    CURRENT_TIMESTAMP() AS loaded_at
FROM deduplicated_sales
