-- models/dimension/dim_date.sql
{{
  config(
    materialized = 'table',
    schema = 'dimension'
  )
}}

WITH date_spine AS (
    SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2022-01-01'::DATE) AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 1386))
),

date_attributes AS (
    SELECT
        date,
        YEAR(date) || RIGHT('0' || MONTH(date), 2) || RIGHT('0' || DAY(date), 2) AS date_id,
        DAY(date) AS day_of_month,
        DAYOFWEEK(date) AS day_of_week,
        CASE DAYOFWEEK(date)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday' 
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_name,
        
        CASE WHEN DAYOFWEEK(date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        
        DAYOFYEAR(date) AS day_of_year,
        
        WEEKOFYEAR(date) AS week_of_year,
        
        MONTH(date) AS month_number,
        CASE MONTH(date)
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS month_name,
        
        QUARTER(date) AS quarter,
        'Q' || QUARTER(date) || '-' || YEAR(date) AS quarter_year,
        
        YEAR(date) AS year,
        
        -- Season definition (Northern Hemisphere)
        CASE 
            WHEN MONTH(date) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(date) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(date) IN (9, 10, 11) THEN 'Fall'
        END AS season,
        
        -- Simplified holiday flag (major US holidays only)
        CASE
            -- New Year's Day
            WHEN MONTH(date) = 1 AND DAY(date) = 1 THEN TRUE
            -- Independence Day
            WHEN MONTH(date) = 7 AND DAY(date) = 4 THEN TRUE
            -- Thanksgiving (fourth Thursday in November)
            WHEN MONTH(date) = 11 AND DAYOFWEEK(date) = 4 
                 AND DATEDIFF('DAY', DATE_TRUNC('MONTH', date), date) BETWEEN 22 AND 28 THEN TRUE
            -- Christmas
            WHEN MONTH(date) = 12 AND DAY(date) = 25 THEN TRUE
            ELSE FALSE
        END AS is_holiday,
        
        -- For time intelligence in reports
        DATEADD(YEAR, -1, date) AS prior_year_date,
        DATEADD(MONTH, -1, date) AS prior_month_date,
        
        -- Add flag for current date to easily identify today in reports
        CASE WHEN date = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_current_date,
        
        CURRENT_TIMESTAMP() AS loaded_at
    FROM date_spine
)

SELECT * FROM date_attributes
ORDER BY date
