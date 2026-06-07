
  
    

    create or replace table `weather-pipeline-498519`.`stg_weather_mart_weather`.`mart_city_comparison`
      
    
    

    OPTIONS()
    as (
      -- models/mart/mart_city_comparison.sql
-- --------------------------------------
-- Latest weather conditions across all 10 cities side by side.
-- Perfect for the "city comparison" dashboard panel.
-- One row per city (most recent snapshot only).

with latest as (

    select
        city_name,
        country,
        latitude,
        longitude,
        observed_at_utc,
        temp_c,
        feels_like_c,
        temp_min_c,
        temp_max_c,
        humidity_pct,
        pressure_hpa,
        wind_speed_mps,
        wind_direction_deg,
        wind_gust_mps,
        weather_category,
        weather_description,
        weather_icon,
        visibility_m,
        cloudiness_pct,
        sunrise_utc,
        sunset_utc,
        ingested_at,

        row_number() over (
            partition by city_name
            order by observed_at_utc desc
        ) as rn

    from `weather-pipeline-498519`.`stg_weather_stg_weather`.`stg_current_weather`

),

ranked as (

    select * from latest where rn = 1

),

-- rank cities by temperature (hottest to coolest)
final as (

    select
        city_name,
        country,
        latitude,
        longitude,
        observed_at_utc,
        temp_c,
        feels_like_c,
        temp_min_c,
        temp_max_c,
        humidity_pct,
        pressure_hpa,
        wind_speed_mps,
        wind_direction_deg,
        wind_gust_mps,
        weather_category,
        weather_description,
        weather_icon,
        visibility_m,
        cloudiness_pct,
        sunrise_utc,
        sunset_utc,
        ingested_at,

        rank() over (order by temp_c desc)         as temp_rank_hottest,
        rank() over (order by humidity_pct desc)   as humidity_rank,
        rank() over (order by wind_speed_mps desc) as wind_rank

    from ranked

)

select * from final
order by temp_rank_hottest
    );
  