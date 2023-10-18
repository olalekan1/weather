{{
  config(
    materialized='view'
  )
}}

SELECT
    JSON_EXTRACT(response, '$.dt') AS timestampUTC,
    JSON_EXTRACT(response, '$.sys.country') AS country,
    JSON_EXTRACT(response, '$.name') AS location,
    JSON_EXTRACT(response, '$.timezone') AS timezone,
    JSON_EXTRACT(response, '$.coord.lon') AS longitude,
    JSON_EXTRACT(response, '$.coord.lat') AS latitude,
    JSON_EXTRACT(response, '$.main.temp') AS temp,
    JSON_EXTRACT(response, '$.main.temp_min') AS temp_min,
    JSON_EXTRACT(response, '$.main.temp_max') AS temp_max,
    JSON_EXTRACT(response, '$.main.feels_like') AS feels_like,
    JSON_EXTRACT(response, '$.main.pressure') AS pressure,
    JSON_EXTRACT(response, '$.main.humidity') AS humidity,
    JSON_EXTRACT(response, '$.wind.speed') AS wind_speed,
    JSON_EXTRACT(response, '$.wind.deg') AS wind_deg,
    JSON_EXTRACT(response, '$.wind.gust') AS wind_gust,
    JSON_EXTRACT_SCALAR(flattened_weather.main) AS weather_main,
    JSON_EXTRACT_SCALAR(flattened_weather.description) AS weather_description
FROM
    {{ source('weather_raw_data', 'weather_info') }},
    UNNEST(JSON_EXTRACT_ARRAY(response, '$.weather')) AS flattened_weather

