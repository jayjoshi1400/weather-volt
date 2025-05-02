{{
  config(
    materialized = 'incremental',
    unique_key = ['balancing_authority_id', 'date_id'],
    schema = 'analytics',
    on_schema_change = 'append_new_columns'
  )
}}

-- Get the demand data 
WITH demand_data AS (
    SELECT
        f.location_id AS balancing_authority_id,
        d.date,
        d.date_id,
        d.day_of_week,
        d.month_number,
        d.is_weekend,
        d.season,
        d.year,
        d.week_of_year,
        d.quarter,  -- Added quarter for better context
        SUM(f.demand_mwh) AS daily_demand_mwh,
        MAX(f.demand_mwh) AS daily_peak_demand_mwh,
        AVG(f.demand_mwh) AS daily_avg_demand_mwh,
        STDDEV(f.demand_mwh) AS daily_demand_variability
    FROM {{ ref('fct_electricity_demand_hourly') }} f
    JOIN {{ ref('dim_date') }} d ON f.date_id = d.date_id
    WHERE {% if is_incremental() %}
        -- Only process new data
        d.date > (SELECT MAX(date) FROM {{ this }})
    {% else %}
        -- Process the last 730 days (2 years) for initial load
        d.date >= DATEADD(day, -730, CURRENT_DATE())
    {% endif %}
    GROUP BY f.location_id, d.date, d.date_id, d.day_of_week, d.month_number, 
             d.is_weekend, d.season, d.year, d.week_of_year, d.quarter
)

SELECT
    balancing_authority_id,
    date_id,
    date,
    day_of_week,
    month_number,
    quarter,
    is_weekend,
    season,
    daily_demand_mwh,
    daily_peak_demand_mwh,
    daily_avg_demand_mwh,
    daily_demand_variability,
    
    -- Trailing averages using window functions
    AVG(daily_demand_mwh) OVER(
        PARTITION BY balancing_authority_id 
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS trailing_7d_avg_demand,
    
    AVG(daily_demand_mwh) OVER(
        PARTITION BY balancing_authority_id 
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS trailing_30d_avg_demand,
    
    AVG(daily_demand_mwh) OVER(
        PARTITION BY balancing_authority_id 
        ORDER BY date 
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS trailing_90d_avg_demand,
    
    -- Quarter-over-quarter percentage change (replacing year-over-year)
    CASE 
        WHEN LAG(daily_demand_mwh, 90) OVER(
            PARTITION BY balancing_authority_id 
            ORDER BY date
        ) IS NOT NULL 
        THEN (daily_demand_mwh - LAG(daily_demand_mwh, 90) OVER(
                PARTITION BY balancing_authority_id 
                ORDER BY date
             )) / NULLIF(LAG(daily_demand_mwh, 90) OVER(
                PARTITION BY balancing_authority_id 
                ORDER BY date
             ), 0) * 100
        ELSE NULL
    END AS quarter_over_quarter_pct_change,
    
    -- Month-over-month percentage change (additional trend metric)
    CASE 
        WHEN LAG(daily_demand_mwh, 30) OVER(
            PARTITION BY balancing_authority_id 
            ORDER BY date
        ) IS NOT NULL 
        THEN (daily_demand_mwh - LAG(daily_demand_mwh, 30) OVER(
                PARTITION BY balancing_authority_id 
                ORDER BY date
             )) / NULLIF(LAG(daily_demand_mwh, 30) OVER(
                PARTITION BY balancing_authority_id 
                ORDER BY date
             ), 0) * 100
        ELSE NULL
    END AS month_over_month_pct_change,
    
    -- Current week and previous week metrics
    AVG(daily_demand_mwh) OVER(
        PARTITION BY balancing_authority_id 
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS current_week_avg,
    
    AVG(daily_demand_mwh) OVER(
        PARTITION BY balancing_authority_id 
        ORDER BY date 
        ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
    ) AS previous_week_avg,
    
    -- Calculate demand trends (rate of change)
    (daily_demand_mwh - LAG(daily_demand_mwh, 7) OVER(
        PARTITION BY balancing_authority_id 
        ORDER BY date
    )) / 7 AS daily_demand_trend_per_day,
    
    -- Calculate weekly pattern strength (how much weekday vs weekend differs)
    AVG(CASE WHEN is_weekend THEN daily_demand_mwh ELSE NULL END) OVER(
        PARTITION BY balancing_authority_id, year, week_of_year
    ) / NULLIF(
        AVG(CASE WHEN NOT is_weekend THEN daily_demand_mwh ELSE NULL END) OVER(
            PARTITION BY balancing_authority_id, year, week_of_year
        ), 0
    ) AS weekend_weekday_ratio,
    
    -- First date to appear in the rolling windows (for context)
    MIN(date) OVER(
        PARTITION BY balancing_authority_id
        ORDER BY date
        ROWS BETWEEN 729 PRECEDING AND CURRENT ROW
    ) AS first_date_in_window,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS processed_at
FROM demand_data
