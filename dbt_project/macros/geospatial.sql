{% macro haversine_distance(lat1, lon1, lat2, lon2) %}
    -- Returns distance in Meters
    ST_DISTANCE(
        ST_MAKEPOINT({{ lon1 }}, {{ lat1 }}),
        ST_MAKEPOINT({{ lon2 }}, {{ lat2 }})
    )
{% endmacro %}

{% macro inside_geofence(lat, lon) %}
    -- Returns BOOLEAN: True if inside DK AIS Zone
    -- Polygon approximates the Danish waters coverage
    ST_CONTAINS(
        TO_GEOGRAPHY('POLYGON((
            3.0 53.0, 
            3.0 60.0, 
            18.0 60.0, 
            18.0 53.0, 
            3.0 53.0
        ))'),
        ST_MAKEPOINT({{ lon }}, {{ lat }})
    )
{% endmacro %}

{% macro knots_to_mps(knots) %}
    {{ knots }} * 0.514444
{% endmacro %}
