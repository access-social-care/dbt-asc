{#
  Fails for every value that appears under more than one spelling once compared
  case-insensitively and trimmed. Replaces the loader's dim guard: Power BI
  compares text case-insensitively and rejects a duplicate key on the
  one-side of a relationship. Use config: severity: warn where native
  variants are tolerated (CSI_DIM).
#}
{% test unique_case_insensitive(model, column_name) %}

select
    lower(trim({{ column_name }})) as normalised_value,
    count(distinct {{ column_name }}) as spellings
from {{ model }}
where {{ column_name }} is not null
group by 1
having count(distinct {{ column_name }}) > 1

{% endtest %}
