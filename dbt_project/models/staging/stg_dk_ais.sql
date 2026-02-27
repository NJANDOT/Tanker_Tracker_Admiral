
WITH source AS (
    SELECT * FROM {{ source('snowflake', 'dk_ais_bronze') }}
),

renamed AS (
    SELECT
        -- IDs
        MMSI AS mmsi,
        IMO AS imo,
        "NAME" AS vessel_name,
        CALLSIGN AS callsign,
        SHIP_TYPE AS ship_type,
        
        -- Geospatial
        LATITUDE AS lat,
        LONGITUDE AS lon,
        
        -- Kinematics
        SOG AS sog_knots,
        COG AS cog,
        HEADING AS heading,
        ROT AS rot,
        
        -- Status & Dimensions
        NAVIGATIONAL_STATUS AS nav_status,
        DRAUGHT AS draught,
        DESTINATION AS destination,
        ETA AS eta,
        
        -- Metadata
        TO_TIMESTAMP(_TIMESTAMP, 'DD/MM/YYYY HH24:MI:SS') AS timestamp,
        DS AS date_partition
        
    FROM source
    WHERE 
        -- Basic Validity Checks
        LATITUDE BETWEEN -90 AND 90
        AND LONGITUDE BETWEEN -180 AND 180
        AND MMSI IS NOT NULL
        AND MMSI != 0
)

SELECT * FROM renamed
