
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select forecast_at_utc
from `weather-pipeline-498519`.`stg_weather_stg_weather`.`stg_forecast_weather`
where forecast_at_utc is null



  
  
      
    ) dbt_internal_test