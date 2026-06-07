
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select temp_rank_hottest
from `weather-pipeline-498519`.`stg_weather_mart_weather`.`mart_city_comparison`
where temp_rank_hottest is null



  
  
      
    ) dbt_internal_test