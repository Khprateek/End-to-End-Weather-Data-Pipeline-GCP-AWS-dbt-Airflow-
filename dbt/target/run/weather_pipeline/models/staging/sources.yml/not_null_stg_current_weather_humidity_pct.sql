
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select humidity_pct
from `weather-pipeline-498519`.`stg_weather_stg_weather`.`stg_current_weather`
where humidity_pct is null



  
  
      
    ) dbt_internal_test