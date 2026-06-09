-- models/mart/mart_city_weather_daily.sql
-- -----------------------------------------
-- Daily weather summary per city.
-- Powers the main dashboard in Looker Studio.
-- One row per city per day.

with daily_agg as (

    select
        city_name,
        country,
        latitude,
        longitude,
        observed_date                               as weather_date,

        -- temperature aggregates
        round(avg(temp_c),  2)                      as avg_temp_c,
        round(min(temp_min_c), 2)                   as min_temp_c,
        round(max(temp_max_c), 2)                   as max_temp_c,
        round(max(temp_max_c) - min(temp_min_c), 2) as temp_range_c,

        -- comfort
        round(avg(feels_like_c), 2)                 as avg_feels_like_c,
        round(avg(humidity_pct), 1)                 as avg_humidity_pct,
        round(avg(pressure_hpa), 1)                 as avg_pressure_hpa,

        -- wind
        round(avg(wind_speed_mps), 2)               as avg_wind_speed_mps,
        round(max(wind_speed_mps), 2)               as max_wind_speed_mps,
        round(avg(
            coalesce(wind_gust_mps, wind_speed_mps)
        ), 2)                                       as avg_wind_gust_mps,

        -- visibility & clouds
        round(avg(visibility_m), 0)                 as avg_visibility_m,
        round(avg(cloudiness_pct), 1)               as avg_cloudiness_pct,

        -- dominant weather condition (most frequent category that day)
        approx_top_count(weather_category, 1)[offset(0)].value
                                                    as dominant_weather,

        -- sun
        min(sunrise_utc)                            as sunrise_utc,
        max(sunset_utc)                             as sunset_utc,

        -- metadata
        count(*)                                    as snapshot_count,
        max(ingested_at)                            as last_ingested_at

    from `weather-pipeline-498519`.`stg_weather`.`stg_current_weather`
    group by 1, 2, 3, 4, 5

)

select
    city_name,
    country,
    latitude,
    longitude,
    weather_date,
    avg_temp_c,
    min_temp_c,
    max_temp_c,
    temp_range_c,
    avg_feels_like_c,
    avg_humidity_pct,
    avg_pressure_hpa,
    avg_wind_speed_mps,
    max_wind_speed_mps,
    avg_wind_gust_mps,
    avg_visibility_m,
    avg_cloudiness_pct,
    dominant_weather,
    sunrise_utc,
    sunset_utc,

    -- derived comfort index (simple heat index approximation)
    round(
        avg_temp_c
        + (0.33 * (avg_humidity_pct / 100.0) * 6.105
            * exp(17.27 * avg_temp_c / (237.7 + avg_temp_c)))
        - 4.0,
    2)                                              as heat_index_c,

    -- hours of daylight
    round(
        timestamp_diff(sunset_utc, sunrise_utc, minute) / 60.0,
    2)                                              as daylight_hours,

    snapshot_count,
    last_ingested_at

from daily_agg
order by weather_date desc, city_name