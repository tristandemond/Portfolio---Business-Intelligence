{% macro get_app_cc_performance(
  client
  ,aggregate_level = 'campaign'
  ,platform_kpi_list = none
  ,sot_kpi_list = none
) -%}

{# aggregate_level can be campaign or ad_group or ad #}

  {%- set segment_names = get_app_cc_segment_names(client) -%}
  {%- set cc_kpis_platform = get_app_cc_platform_kpis(client, platform_kpi_list) -%}
  {%- set cc_kpis_sot = get_app_cc_analytics_kpis(client, sot_kpi_list) -%}

  {%- set model_names = [
      ref('stg_app__cc_delivery__' ~ client),
      ref('stg_app__cc_platform_performance__' ~ client),
      ref('stg_app__cc_analytics_performance__' ~ client)
  ] -%}

with
  unioned as (
    {{ dbt_utils.union_relations(
        relations=model_names
    ) }}
  )

  ,aggregated as (
    select
      date
      ,source
      ,platform_source
      ,account_id
      ,account_name
      ,platform
      ,platform_id
      ,channel
      ,channel_id
      ,surface
      ,surface_id

      ,campaign_id
      ,campaign_name

      {%- if aggregate_level == 'ad_group' -%}
      ,ad_group_id
      ,ad_group_name
      {%- endif -%}

      {%- if aggregate_level == 'ad' -%}
      ,ad_id
      ,ad_name
      {%- endif -%}
   
      {% for segment_name in segment_names %}
      ,"{{ segment_name | trim | lower | replace(' ', '_') }}"
      {% endfor %}

      ,sum(impressions) as impressions
      ,sum(clicks) as clicks
      ,sum(spend) as spend

      {% for kpi_platform in cc_kpis_platform -%}
      ,sum("platform__{{ kpi_platform | trim | lower | replace(' ', '_') }}__volume") as "platform__{{ kpi_platform | trim | lower | replace(' ', '_') }}__volume"
      ,sum("platform__{{ kpi_platform | trim | lower | replace(' ', '_') }}__revenue") as "platform__{{ kpi_platform | trim | lower | replace(' ', '_') }}__revenue"
      {% endfor -%}

      {% for kpi_sot in cc_kpis_sot -%}
      ,sum("sot__{{ kpi_sot[0] }}__{{ kpi_sot[1] | trim | lower | replace(' ', '_') }}__volume") as "sot__{{ kpi_sot[0] }}__{{ kpi_sot[1] | trim | lower | replace(' ', '_') }}__volume"
      ,sum("sot__{{ kpi_sot[0] }}__{{ kpi_sot[1] | trim | lower | replace(' ', '_') }}__revenue") as "sot__{{ kpi_sot[0] }}__{{ kpi_sot[1] | trim | lower | replace(' ', '_') }}__revenue"
      {% endfor %}
     
    from unioned
    {{ dbt_utils.group_by(segment_names|length + 13) if aggregate_level == 'campaign'}}
    {{ dbt_utils.group_by(segment_names|length + 15) if aggregate_level == 'ad_group'}}
    {{ dbt_utils.group_by(segment_names|length + 17) if aggregate_level == 'ad'}}
  )

select *

from aggregated

{%- endmacro %}