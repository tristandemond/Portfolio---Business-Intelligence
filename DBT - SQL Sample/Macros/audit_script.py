{% macro compare_which_column_values_differs(
  client
  ,relation_name
  ,list_dims = []
  ,list_metrics = []
  ,exclude_columns = []
  ,pivotted = 'True'
  ,filter_clause="1=1"
  ,verbose=false
) %}

  {#- set development relation name -#}
  {%- set dev_relation = adapter.get_relation(
      database = this.database,
      schema = this.schema,
      identifier = relation_name
  ) -%}

  {%- if verbose -%}
    {{ log('DEV relation name --> ' ~ dev_relation, info=True) }}
  {%- endif -%}

  {#- set production relation name -#}
  {%- if relation_name.startswith('rpt') or relation_name.startswith('prep') -%}
    {%- set schema_name = client -%}
  {%- else -%}
    {%- set schema_name = client ~ '_stg' -%}
  {%- endif -%}

  {%- set prod_relation = 'tinuiti.' ~ schema_name ~ '.' ~ relation_name -%}

  {%- if verbose -%}
    {{ log('PROD relation name --> ' ~ prod_relation, info=True) }}
  {%- endif -%}

  {#- if metrics and dimensions are not passed, set list_dims and list_metrics -#}
  {%- if (list_dims is none or list_dims == []) or (list_metrics is none or list_metrics == []) -%}
    {%- set cols_dims = [] -%}
    {%- set cols_metrics = [] -%}
    
    {%- set query -%}
      select 
        column_name::varchar
        ,case
          when data_type ~ 'int|double|numeric|real|super' then 'Metric'
          else 'Dimension'
        end::varchar as column_type

      from information_schema.columns c 
      where table_schema = '{{ this.schema }}'
      and table_name = '{{ relation_name }}'
      order by 2,1
    {%- endset -%}

    --Run the query and capture the results
    {%- set results = run_query(query) -%}

    --Set default column list which needs to be excluded
    {%- set exclude_list = ['date','report_date','insert_date','_dw_insert_date'] + exclude_columns  -%}

    {%- if execute -%}
    {%- for row in results.rows -%}
      {%- set column_name = row[0] -%}
      {%- set column_type = row[1] -%}
      
      --Check if column_type is 'Dimension'/'Metric', and if so, append to cols_dims or cols_metrics
      {%- if column_type == 'Dimension' and column_name not in exclude_list -%}
          {%- do cols_dims.append(column_name) -%}
      {%- endif -%}
      {%- if column_type == 'Metric' -%}
          {%- do cols_metrics.append(column_name) -%}
      {%- endif -%}
    {%- endfor -%}
    {%- endif -%}

  {%- else -%}
    {%- set cols_dims = list_dims -%}
    {%- set cols_metrics = list_metrics -%}
  {%- endif -%}

  {%- if verbose -%}
    {{ log('List of Dimensions being compared --> ' ~ cols_dims, info=True) }}
    {{ log('List of Metrics being compared --> ' ~ cols_metrics, info=True) }}
  {%- endif -%}

  {# Throw compilation error if there are no segments #}
  {%-if cols_dims is none or cols_dims|length == 0 -%}
    {%- if verbose -%}
      {{ log('ERROR: Input relation is a view. Either specify list_dims and list_metrics or pass a table relation', info=True) }}
    {%- endif -%}
    {%-do return(None)-%}
  {%- endif -%}

  {#- create full list of fields -#}
  {%- set columns = cols_dims + cols_metrics %}
  {%- set where_clause = filter_clause|replace("''","'") %}
  with 
    source_dev as (
      {%- for dim in cols_dims %}
      select
        {% set quoted_dim = adapter.quote(dim) -%}
        'dev' as table_environment,
        '{{ dim }}'::varchar as dim_column_name,
        nvl({{ quoted_dim }},'NULL') as dim_column_value,

        {%- for metric in cols_metrics -%}

        {% set quoted_metric = adapter.quote(metric) %}
        round(sum({{ quoted_metric }}), 2) as sum_{{ metric }}
        {%- if not loop.last %}, {% endif %}

        {%- endfor %}
        
      from {{dev_relation}}
      where {{ where_clause }}
      {{ dbt_utils.group_by(3) }}
      {% if not loop.last %}
      union all

      {% endif -%}
      {%- endfor -%}
    ),

    source_prod as (
      {%- for dim in cols_dims %}
      select
        {% set quoted_dim = adapter.quote(dim) -%}
        'prod' as table_environment,
        '{{ dim }}'::varchar as dim_column_name,
        nvl({{ quoted_dim }},'NULL') as dim_column_value,

        {%- for metric in cols_metrics -%}

        {% set quoted_metric = adapter.quote(metric) %}
        round(sum({{ quoted_metric }}), 2) as sum_{{ metric }}
        {%- if not loop.last %}, {% endif %}

        {%- endfor %}
        
      from {{prod_relation}}
      where {{ where_clause }}
      {{ dbt_utils.group_by(3) }}
      {% if not loop.last %}
      union all

      {% endif -%}
      {%- endfor %}
    ),

    calculated as (
      select
        coalesce(a.dim_column_name, b.dim_column_name) as dim_column_name,
        a.dim_column_value as dev_segment_value,
        b.dim_column_value as prod_segment_value,

        {%- for metric in cols_metrics %}
        {% set metric_name = 'sum_' ~ metric %}
        {%- set quoted_metric = adapter.quote(metric_name) -%}
        (round(nvl(a.{{ quoted_metric }},0), 2) != round(nvl(b.{{ quoted_metric }},0), 2)) as {{ metric | lower }}_has_difference,
        a.{{ quoted_metric }} as {{ metric | lower }}_dev_value,
        b.{{ quoted_metric }} as {{ metric | lower }}_prod_value,
        (b.{{ quoted_metric }} - a.{{ quoted_metric }}) as {{ metric | lower }}_diff
        {%- if not loop.last %}, {% endif %}
        {% endfor %}
          
      from source_dev a
      full outer join source_prod b 
        on a.dim_column_name = b.dim_column_name
        and a.dim_column_value = b.dim_column_value
      order by 1,3
    )

{%- if pivotted == 'True' -%}

  {%- for metric in cols_metrics %}
  
  select 
    dim_column_name,
    dev_segment_value,
    prod_segment_value,
    '{{ metric }}' as metric_name, 
    {{ metric | lower }}_has_difference as has_difference,
    {{ metric | lower }}_dev_value as dev_value,
    {{ metric | lower }}_prod_value as prod_value,
    {{ metric | lower }}_diff as prod_vs_dev_diff,
    case
      when {{ metric | lower }}_prod_value > 0 then ({{ metric | lower }}_diff/{{ metric | lower }}_prod_value)
      else 0
    end as prod_vs_dev_pct_diff
    
  from calculated

  {%- if not loop.last %}
      
  union all 

  {%- endif -%}

  {%- endfor -%}

{%- else -%}

  select *

  from calculated

{%- endif -%}

    

{% endmacro %}
