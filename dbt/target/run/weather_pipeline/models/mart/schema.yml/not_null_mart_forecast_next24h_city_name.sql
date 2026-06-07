
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select city_name
from `weather-pipeline-498519`.`stg_weather_mart_weather`.`mart_forecast_next24h`
where city_name is null



  
  
      
    ) dbt_internal_test