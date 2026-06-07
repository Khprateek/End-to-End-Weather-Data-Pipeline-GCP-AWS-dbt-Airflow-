
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select temp_c
from `weather-pipeline-498519`.`stg_weather_stg_weather`.`stg_current_weather`
where temp_c is null



  
  
      
    ) dbt_internal_test