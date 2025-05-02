{{
  config(
    materialized = 'table',
    schema = 'dimension'
  )
}}

-- Your bridge table definition here
SELECT 
    ba.location_id AS balancing_authority_id,
    ba.city AS balancing_authority_name,
    ba.location_code AS ba_code,
    ws.location_id AS weather_station_id,
    ws.city AS weather_station_name,
    ws.latitude AS weather_station_lat,
    ws.longitude AS weather_station_long
FROM {{ ref('dim_location') }} ba
CROSS JOIN {{ ref('dim_location') }} ws
WHERE ba.location_type = 'Balancing Authority'
  AND ws.location_type = 'Weather Station'
  AND ba.state_name = ws.state_name
  AND ws.latitude IS NOT NULL
  AND (
      (ba.location_code = 'DUK' AND ws.city = 'Charlotte Douglas International Airport')
      OR
      (ba.location_code = 'CPLE' AND ws.city = 'Raleigh-Durham International Airport')
  )
