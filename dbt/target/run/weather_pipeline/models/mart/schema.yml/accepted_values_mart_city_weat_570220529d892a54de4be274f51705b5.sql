
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        dominant_weather as value_field,
        count(*) as n_records

    from `weather-pipeline-498519`.`stg_weather_mart_weather`.`mart_city_weather_daily`
    group by dominant_weather

)

select *
from all_values
where value_field not in (
    'Clear','Clouds','Rain','Drizzle','Thunderstorm','Snow','Mist','Smoke','Haze','Dust','Fog','Sand','Ash','Squall','Tornado'
)



  
  
      
    ) dbt_internal_test