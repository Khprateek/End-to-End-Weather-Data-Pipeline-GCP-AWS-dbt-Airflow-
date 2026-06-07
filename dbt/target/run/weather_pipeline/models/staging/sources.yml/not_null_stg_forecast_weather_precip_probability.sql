
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select precip_probability
from `weather-pipeline-498519`.`stg_weather_stg_weather`.`stg_forecast_weather`
where precip_probability is null



  
  
      
    ) dbt_internal_test