
    
    



select forecast_at_utc
from `weather-pipeline-498519`.`stg_weather_mart_weather`.`mart_forecast_next24h`
where forecast_at_utc is null


