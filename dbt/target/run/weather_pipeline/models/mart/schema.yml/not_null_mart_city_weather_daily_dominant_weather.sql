
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dominant_weather
from `weather-pipeline-498519`.`stg_weather_mart_weather`.`mart_city_weather_daily`
where dominant_weather is null



  
  
      
    ) dbt_internal_test