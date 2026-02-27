
WITH source AS (
    SELECT * FROM {{ source('snowflake', 'bronze_sanctions') }}
),

clean AS (
    SELECT
        MMSI
        ,IMO
        ,ID AS sanction_id
        ,"TYPE" AS schema_type
        ,CAPTION AS vessel_name
        ,COUNTRIES AS country
    FROM source
    WHERE schema_type = 'Vessel'
)

SELECT * FROM clean
