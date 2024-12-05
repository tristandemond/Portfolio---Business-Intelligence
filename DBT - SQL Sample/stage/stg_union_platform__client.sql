{{ config(
      tags=['client', 'stage']
) }}

with
  unioned as (
    {{ dbt_utils.union_relations(
        relations=[
          ref('stg_google_ads__client')
          ,ref('stg_facebook__client')
          ,ref('stg_bing__client')
          ,ref('stg_ga__client')
          ,ref('stg_marketplace_instacart__client')
          ,ref('stg_marketplace_amazon__client')
          ,ref('stg_marketplace_walmart__client')
          ,ref('stg_sprout__client')
          ,ref('stg_marketplace_target__client')
          ,ref('stg_attentive__client')
          ]
    ) }}
  )

, aggregated as (
  select 
    date
    ,engine
    ,channel
    ,campaign_branded
    ,connection
    ,_dw_connection
    ,product_category
    ,tactic
    ,state
    ,utm_content
    ,utm_campaign
    ,product
    ,device
    ,utm_source
    ,ad_group
    ,ad_creative
    ,ad
    ,campaign
    ,utm_medium
    ,max(insert_date) as insert_date

    ,sum(conv_ntb) as conv_ntb
    ,sum(rev_ntb) as rev_ntb
    ,sum(new_user_sessions) as new_user_sessions
    ,sum(revenue_platform) as revenue_platform
    ,sum(spend) as spend
    ,sum(link_clicks) as link_clicks
    ,sum(video_views) as video_views
    ,sum(engagement) as engagement
    ,sum(comments) as comments
    ,sum(conversions_platform) as conversions_platform
    ,sum(impressions) as impressions
    ,sum(clicks) as clicks
    ,sum(sessions) as sessions
    ,sum(bounces) as bounces
    ,sum(pageviews) as pageviews
    ,sum(revenue_ntb) as revenue_ntb
    ,sum(product_revenue) as product_revenue
    ,sum(conversions_ntb) as conversions_ntb
    ,sum(conversions_ga) as conversions_ga
    ,sum(revenue_ga) as revenue_ga
    ,sum(unique_purchases) unique_purchases
    ,sum(item_quantity) as item_quantity
    ,sum(eligible_impressions) as eligible_impressions
    ,sum(impressions_for_calc_use_only) as impressions_for_calc_use_only

  from unioned
  {{ dbt_utils.group_by(19) }}
  )

select *
from aggregated