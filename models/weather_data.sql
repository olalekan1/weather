{{
  config(
    materialized='view'
  )
}}

SELECT
    TIMESTAMP_SECONDS(CAST(JSON_EXTRACT_SCALAR(response, '$.dt') AS INT64)) AS timestampUTC,
    JSON_EXTRACT_SCALAR(response, '$.sys.country') AS country,
    JSON_EXTRACT_SCALAR(response, '$.name') AS location,
    CAST(JSON_EXTRACT_SCALAR(response, '$.timezone') AS NUMERIC) AS timezone,
    JSON_EXTRACT_SCALAR(response, '$.coord.lon') AS longitude,
    JSON_EXTRACT_SCALAR(response, '$.coord.lat') AS latitude,
    JSON_EXTRACT_SCALAR(response, '$.main.temp') AS temp,
    JSON_EXTRACT_SCALAR(response, '$.main.temp_min') AS temp_min,
    JSON_EXTRACT_SCALAR(response, '$.main.temp_max') AS temp_max,
    JSON_EXTRACT_SCALAR(response, '$.main.feels_like') AS feels_like,
    JSON_EXTRACT_SCALAR(response, '$.main.pressure') AS pressure,
    JSON_EXTRACT_SCALAR(response, '$.main.humidity') AS humidity,
    JSON_EXTRACT_SCALAR(response, '$.wind.speed') AS wind_speed,
    JSON_EXTRACT_SCALAR(response, '$.wind.deg') AS wind_deg,
    JSON_EXTRACT_SCALAR(response, '$.wind.gust') AS wind_gust,
    JSON_EXTRACT_SCALAR(flattened_weather.main) AS weather_main,
    JSON_EXTRACT_SCALAR(flattened_weather.description) AS weather_description
FROM
    {{ source('weather_raw_data', 'weather_info') }},
    UNNEST(JSON_EXTRACT_ARRAY(response, '$.weather')) AS flattened_weather

