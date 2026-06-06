-- macros/test_temperature_in_range.sql
-- --------------------------------------
-- Custom generic dbt test.
-- Usage in schema.yml:
--   tests:
--     - temperature_in_range:
--         min_temp: -20
--         max_temp: 55
--
-- Fails if any row has a temperature outside the given range.

{% test temperature_in_range(model, column_name, min_temp=-20, max_temp=55) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and (
      {{ column_name }} < {{ min_temp }}
   or {{ column_name }} > {{ max_temp }}
  )

{% endtest %}
