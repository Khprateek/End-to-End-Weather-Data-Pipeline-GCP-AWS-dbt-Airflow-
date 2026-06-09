
  
    

    create or replace table `weather-pipeline-498519`.`mart_weather`.`mart_forecast_next24h`
      
    
    

    OPTIONS()
    as (
      -- models/mart/mart_forecast_next24h.sql
-- ---------------------------------------
-- Next 24 hours of forecast per city, aggregated to hourly slots.
-- Powers the "what's coming" panel in the dashboard.

with hourly as (

    select
        city_name,
        country,
        forecast_at_utc,
        forecast_date,
        forecast_hour,
        temp_c                  as forecast_temp_c,
        temp_min_c              as forecast_temp_min_c,
        temp_max_c              as forecast_temp_max_c,
        humidity_pct            as forecast_humidity_pct,
        wind_speed_mps          as forecast_wind_speed_mps,
        weather_category        as forecast_weather_category,
        precip_probability      as forecast_precip_prob,
        rain_3h_mm              as forecast_rain_3h_mm,
        ingested_at,

        -- flag the peak temperature hour per city per day
        rank() over (
            partition by city_name, forecast_date
            order by temp_c desc
        ) as peak_temp_rank

    from `weather-pipeline-498519`.`stg_weather`.`stg_forecast_weather`
    where forecast_at_utc >= timestamp_trunc(current_timestamp(), hour)
      and forecast_at_utc <  timestamp_add(
              timestamp_trunc(current_timestamp(), hour), interval 24 hour)

)

select
    city_name,
    country,
    forecast_at_utc,
    forecast_date,
    forecast_hour,
    forecast_temp_c,
    forecast_temp_min_c,
    forecast_temp_max_c,
    forecast_humidity_pct,
    forecast_wind_speed_mps,
    forecast_weather_category,
    round(forecast_precip_prob * 100, 1)    as precip_probability_pct,
    forecast_rain_3h_mm,
    peak_temp_rank = 1                      as is_peak_temp_hour,
    ingested_at

from hourly
order by city_name, forecast_at_utc
    );
  