
{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='append',
    pre_hook="DELETE FROM {{ this }} WHERE date_partition = '{{ var(\"target_date\", \"2024-01-01\") }}'"
  )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_dk_ais') }}
    {% if is_incremental() %}
    -- Load Target Date AND previous day (for LAG calculation)
    WHERE date_partition >= DATEADD(day, -1, '{{ var("target_date", "2024-01-01") }}')
    AND date_partition <= '{{ var("target_date", "2024-01-01") }}'
    {% endif %}
),

lagged AS (
    SELECT
        *,
        -- Previous State (Kinematics)
        LAG(lat) OVER (PARTITION BY mmsi ORDER BY timestamp) AS prev_lat,
        LAG(lon) OVER (PARTITION BY mmsi ORDER BY timestamp) AS prev_lon,
        LAG(timestamp) OVER (PARTITION BY mmsi ORDER BY timestamp) AS prev_timestamp,
        
        -- Previous State (Events)
        LAG(draught) OVER (PARTITION BY mmsi ORDER BY timestamp) AS prev_draught,
        LAG(nav_status) OVER (PARTITION BY mmsi ORDER BY timestamp) AS prev_status
    FROM staging
),

calculated AS (
    SELECT
        *,
        -- Time Delta (Seconds)
        COALESCE(DATEDIFF(second, prev_timestamp, timestamp), 0) AS delta_time_sec,
        
        -- Distance Delta (Meters)
        {{ tanker_brew_admiral.haversine_distance('lat', 'lon', 'prev_lat', 'prev_lon') }} AS delta_dist_meters
    FROM lagged
),

enriched AS (
    SELECT 
        *,
        -- Surrogate Key for Unique Test / Incremental Merge
        {{ dbt_utils.generate_surrogate_key(['mmsi', 'timestamp']) }} as id,
        
        -- Calculated Speed (Knots) => (Meters / Seconds) * 1.94384
        CASE 
            WHEN delta_time_sec > 60 THEN (delta_dist_meters / delta_time_sec) * 1.94384 
            -- Ignore small time deltas (< 60s) to avoid absurd speeds caused by GPS jitter/noise
            ELSE 0 
        END AS calc_speed_knots,
        
        -- Geofencing Check
        {{ tanker_brew_admiral.inside_geofence('lat', 'lon') }} AS is_in_dk_zone
    FROM calculated
)

SELECT * FROM enriched
{% if is_incremental() %}
WHERE date_partition = '{{ var("target_date", "2024-01-01") }}'
{% endif %}
