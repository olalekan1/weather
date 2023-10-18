SELECT
    *
FROM
    {{ source('weather_raw_data', 'weather_info') }}
    --airflowdbt-396514.weather.weather_info

/*
delete
    --*
FROM
    {{ source('weather_raw_data', 'weather_info') }}
    --airflowdbt-396514.weather.weather_info
where
    JSON_EXTRACT_SCALAR(response, '$.name') = "Peter"
*/