{{ config(
      tags=['client', 'stage']
) }}

with
  unioned as (
    {{ dbt_utils.union_relations(
        relations=[
          ref('stg_union_platform__client')
          ,ref('stg_union_programmatic__client')
          ,ref('stg_tiktok__client')
          ,ref('stg_sizmek__client')
          ,ref('stg_spotify__client')
          ]
    ) }}
  )

  , filtered as (
    select 
    date
    ,device
    ,tactic
    ,product_category
    ,engine
    ,channel
    ,campaign_branded
    ,connection
    ,_dw_connection
    ,utm_content
    ,campaign
    ,product
    ,ad_group
    ,utm_campaign
    ,utm_medium
    ,utm_source
    ,ad_creative
    ,state
    ,ad
    ,max(insert_date) as insert_date

    ,sum(conv_ntb) as conv_ntb
    ,sum(rev_ntb) rev_ntb
    ,sum(new_user_sessions) as new_user_sessions
    ,sum(conversions_platform) as conversions_platform
    ,sum(impressions) as impressions
    ,sum(clicks) as clicks
    ,sum(link_clicks) as link_clicks
    ,sum(engagement) as engagement
    ,sum(comments) as comments
    ,sum(sessions) as sessions
    ,sum(bounces) as bounces
    ,sum(revenue_platform) as revenue_platform
    ,sum(spend) as spend
    ,sum(conversions_ntb) conversions_ntb
    ,sum(revenue_ntb) as revenue_ntb
    ,sum(conversions_ga) as conversions_ga
    ,sum(revenue_ga) as revenue_ga
    ,sum(unique_purchases) as unique_purchases
    ,sum(product_revenue) as product_revenue
    ,sum(item_quantity) as item_quantity
    ,sum(pageviews) as pageviews
    ,sum(video_views) as video_views
    ,sum(eligible_impressions) as eligible_impressions
    ,sum(impressions_for_calc_use_only) as impressions_for_calc_use_only

    from unioned
    {{ dbt_utils.group_by(19) }}
    
  )

select *
from filtered