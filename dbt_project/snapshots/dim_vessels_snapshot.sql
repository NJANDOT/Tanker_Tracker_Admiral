
{% snapshot dim_vessels_snapshot %}

{{
    config(
      target_database='DATAEXPERT_STUDENT',
      target_schema='NICOLASSTEEL',
      unique_key='mmsi',

      strategy='check',
      check_cols=['vessel_name', 'callsign', 'ship_type', 'imo'],
    )
}}

WITH source AS (
    SELECT
        mmsi,
        vessel_name,
        callsign,
        ship_type,
        imo,
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY timestamp DESC) as rn
    FROM {{ ref('stg_dk_ais') }}
)

SELECT
    mmsi,
    vessel_name,
    callsign,
    ship_type,
    imo
FROM source
WHERE rn = 1

{% endsnapshot %}
