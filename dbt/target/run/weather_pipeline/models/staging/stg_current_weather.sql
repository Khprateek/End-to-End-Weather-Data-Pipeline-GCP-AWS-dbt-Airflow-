

  create or replace view `weather-pipeline-498519`.`stg_weather`.`stg_current_weather`
  OPTIONS()
  as -- models/staging/stg_current_weather.sql
-- ----------------------------------------
-- Cleans and types the raw current_weather table.
-- One row per city per hourly snapshot.
-- Materialized as a view (always fresh, zero storage cost).

with source as (

    select * from `weather-pipeline-498519`.`raw_weather`.`current_weather`

),

renamed as (

    select
        -- identifiers
        city_id                                         as city_id,
        city_name                                       as city_name,
        country                                         as country,
        pipeline_city_query                             as city_query,

        -- location
        round(cast(lat as float64), 4)                  as latitude,
        round(cast(lon as float64), 4)                  as longitude,

        -- time
        cast(dt as int64)                               as epoch_ts,
        timestamp_seconds(cast(dt as int64))            as observed_at_utc,
        date(timestamp_seconds(cast(dt as int64)))      as observed_date,
        extract(hour from timestamp_seconds(
            cast(dt as int64)))                         as observed_hour,
        cast(ingested_at as timestamp)                  as ingested_at,

        -- temperature (already in Celsius from extractor)
        round(cast(temp        as float64), 2)          as temp_c,
        round(cast(feels_like  as float64), 2)          as feels_like_c,
        round(cast(temp_min    as float64), 2)          as temp_min_c,
        round(cast(temp_max    as float64), 2)          as temp_max_c,

        -- atmospheric
        cast(pressure  as int64)                        as pressure_hpa,
        cast(humidity  as int64)                        as humidity_pct,
        cast(visibility as int64)                       as visibility_m,
        cast(cloudiness as int64)                       as cloudiness_pct,

        -- wind
        round(cast(wind_speed as float64), 2)           as wind_speed_mps,
        cast(wind_deg as int64)                         as wind_direction_deg,
        round(cast(wind_gust  as float64), 2)           as wind_gust_mps,

        -- weather condition
        cast(weather_id   as int64)                     as weather_code,
        weather_main                                    as weather_category,
        weather_desc                                    as weather_description,
        weather_icon                                    as weather_icon,

        -- sun
        timestamp_seconds(cast(sunrise as int64))       as sunrise_utc,
        timestamp_seconds(cast(sunset  as int64))       as sunset_utc

    from source
    where city_name is not null
      and dt        is not null

)

select * from renamed;

