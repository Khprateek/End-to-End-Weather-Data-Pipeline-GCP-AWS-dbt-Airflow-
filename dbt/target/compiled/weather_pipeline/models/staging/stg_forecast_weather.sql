-- models/staging/stg_forecast_weather.sql
-- -----------------------------------------
-- Cleans and types the raw forecast_weather table.
-- One row per 3-hour forecast interval per city.
-- Materialized as a view.

with source as (

    select * from `weather-pipeline-498519`.`raw_weather`.`forecast_weather`

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
        timestamp_seconds(cast(dt as int64))            as forecast_at_utc,
        date(timestamp_seconds(cast(dt as int64)))      as forecast_date,
        extract(hour from timestamp_seconds(
            cast(dt as int64)))                         as forecast_hour,
        dt_txt                                          as forecast_dt_txt,
        cast(ingested_at as timestamp)                  as ingested_at,

        -- temperature
        round(cast(temp       as float64), 2)           as temp_c,
        round(cast(feels_like as float64), 2)           as feels_like_c,
        round(cast(temp_min   as float64), 2)           as temp_min_c,
        round(cast(temp_max   as float64), 2)           as temp_max_c,

        -- atmospheric
        cast(pressure   as int64)                       as pressure_hpa,
        cast(humidity   as int64)                       as humidity_pct,
        cast(cloudiness as int64)                       as cloudiness_pct,

        -- wind
        round(cast(wind_speed as float64), 2)           as wind_speed_mps,
        cast(wind_deg as int64)                         as wind_direction_deg,
        round(cast(wind_gust  as float64), 2)           as wind_gust_mps,

        -- weather condition
        weather_main                                    as weather_category,
        weather_desc                                    as weather_description,

        -- precipitation
        round(cast(pop     as float64), 4)              as precip_probability,
        round(coalesce(cast(rain_3h as float64), 0), 2) as rain_3h_mm,
        round(coalesce(cast(snow_3h as float64), 0), 2) as snow_3h_mm

    from source
    where city_name is not null
      and dt        is not null

)

select * from renamed