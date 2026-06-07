
    
    

with dbt_test__target as (

  select city_name as unique_field
  from `weather-pipeline-498519`.`stg_weather_mart_weather`.`mart_city_comparison`
  where city_name is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


