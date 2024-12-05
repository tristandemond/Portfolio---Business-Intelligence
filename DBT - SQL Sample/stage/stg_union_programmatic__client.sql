{{ config(
      tags=['client', 'stage']
) }}

with
  unioned as (
    {{ dbt_utils.union_relations(
        relations=[
          ref('stg_ttd__client')
          ,ref('stg_klaviyo__client')
          ,ref('stg_dotdash__client')
          ,ref('stg_triplelift__client')
          ]
    ) }}
  )

, aggregated as (
  select 
    date
    ,tactic
    ,connection
    ,_dw_connection
    ,channel
    ,engine
    ,ad
    ,campaign
    ,ad_creative
    ,campaign_branded
    ,ad_group
    ,max(insert_date) as insert_date

    ,sum(pageviews) as pageviews
    ,sum(impressions) as impressions
    ,sum(clicks) as clicks
    ,sum(clicks) as clicks_ga
    ,sum(spend) as spend
    ,sum(conversions_platform) as conversions_platform
    ,sum(bounces) as bounces
    ,sum(revenue_platform) as revenue_platform

    from unioned
    {{ dbt_utils.group_by(11) }}
)

select *

from aggregated
