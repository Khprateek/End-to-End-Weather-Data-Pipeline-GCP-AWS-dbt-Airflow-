-- models/intermediate/int_weather_combined.sql
-- ----------------------------------------------
-- Combines the latest current snapshot per city with the next 24h
-- of forecast intervals. Used as input to all mart models.
-- Materialized as a table (refreshed on each dbt run).

with current_snapshots as (

    select
        city_name,
        city_id,
        country,
        latitude,
        longitude,
        observed_at_utc,
        observed_date,
        observed_hour,
        temp_c,
        feels_like_c,
        temp_min_c,
        temp_max_c,
        pressure_hpa,
        humidity_pct,
        visibility_m,
        cloudiness_pct,
        wind_speed_mps,
        wind_direction_deg,
        wind_gust_mps,
        weather_code,
        weather_category,
        weather_description,
        sunrise_utc,
        sunset_utc,
        ingested_at,

        -- rank snapshots per city, most recent first
        row_number() over (
            partition by city_name
            order by observed_at_utc desc
        ) as recency_rank

    from {{ ref('stg_current_weather') }}

),

latest_current as (

    select * from current_snapshots
    where recency_rank = 1

),

forecast as (

    select
        city_name,
        forecast_at_utc,
        forecast_date,
        forecast_hour,
        temp_c             as forecast_temp_c,
        temp_min_c         as forecast_temp_min_c,
        temp_max_c         as forecast_temp_max_c,
        humidity_pct       as forecast_humidity_pct,
        wind_speed_mps     as forecast_wind_speed_mps,
        weather_category   as forecast_weather_category,
        precip_probability as forecast_precip_prob,
        rain_3h_mm         as forecast_rain_3h_mm

    from {{ ref('stg_forecast_weather') }}
    -- only keep the next 24h of forecasts from latest ingestion
    where forecast_at_utc >= timestamp_trunc(current_timestamp(), hour)
      and forecast_at_utc <  timestamp_add(
              timestamp_trunc(current_timestamp(), hour), interval 24 hour)

)

select
    lc.city_name,
    lc.city_id,
    lc.country,
    lc.latitude,
    lc.longitude,

    -- current conditions
    lc.observed_at_utc,
    lc.observed_date,
    lc.temp_c                   as current_temp_c,
    lc.feels_like_c             as current_feels_like_c,
    lc.humidity_pct             as current_humidity_pct,
    lc.pressure_hpa             as current_pressure_hpa,
    lc.wind_speed_mps           as current_wind_speed_mps,
    lc.wind_direction_deg       as current_wind_direction_deg,
    lc.weather_category         as current_weather_category,
    lc.weather_description      as current_weather_description,
    lc.visibility_m             as current_visibility_m,
    lc.cloudiness_pct           as current_cloudiness_pct,
    lc.sunrise_utc,
    lc.sunset_utc,

    -- next 24h forecast
    fc.forecast_at_utc,
    fc.forecast_hour,
    fc.forecast_temp_c,
    fc.forecast_temp_min_c,
    fc.forecast_temp_max_c,
    fc.forecast_humidity_pct,
    fc.forecast_wind_speed_mps,
    fc.forecast_weather_category,
    fc.forecast_precip_prob,
    fc.forecast_rain_3h_mm,

    lc.ingested_at

from latest_current lc
left join forecast   fc on lc.city_name = fc.city_name
