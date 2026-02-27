
{{
  config(
    materialized='incremental',
    unique_key=['mmsi', 'activity_date'],
    incremental_strategy='delete+insert'
  )
}}

SELECT
    -- Dimensions
    t.mmsi,
    t.date_partition AS activity_date,
    
    -- Metadata (Pick one if multiple exist per day to avoid duplicates)
    MAX(t.vessel_name) as vessel_name,

    -- Metrics
    ROUND(SUM(delta_dist_meters) * 0.000539957, 2) AS total_dist_nm, -- Meters to NM
    ROUND(AVG(calc_speed_knots), 2) AS avg_speed_kts,
    ROUND(MAX(calc_speed_knots), 2) AS max_speed_kts,

    -- Business Logic / Anomalies
    MAX(CASE WHEN calc_speed_knots > 100 THEN TRUE ELSE FALSE END) AS speed_anomaly,
    MAX(CASE WHEN delta_time_sec > 7200 THEN TRUE ELSE FALSE END) AS has_blackouts_greater_than_2hrs,
    (MAX(draught) - MIN(draught)) > 0.5 AS has_draught_change,

    MAX(CASE WHEN s.sanction_id IS NOT NULL THEN TRUE ELSE FALSE END) AS is_sanctioned,
    MAX(s.country) AS sanction_country

FROM {{ ref('int_ais_trajectories') }} t
LEFT JOIN {{ ref('stg_sanctions') }} s 
    ON t.mmsi = s.mmsi
{% if is_incremental() %}
WHERE t.date_partition = '{{ var("target_date", "2024-01-01") }}'
{% endif %}
GROUP BY 1, 2
