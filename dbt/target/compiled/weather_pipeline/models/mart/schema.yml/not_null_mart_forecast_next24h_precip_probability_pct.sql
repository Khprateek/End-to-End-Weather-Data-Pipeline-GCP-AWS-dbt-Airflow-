
    
    



select precip_probability_pct
from `weather-pipeline-498519`.`stg_weather_mart_weather`.`mart_forecast_next24h`
where precip_probability_pct is null


