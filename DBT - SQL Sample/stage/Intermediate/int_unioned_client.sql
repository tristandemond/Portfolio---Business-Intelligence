{{ config(
      tags=['client', 'intermediate']
) }}

with
  source as (
    select *
    from {{ ref('stg_union_allplatforms__client') }}
  )

, transformed_level1 as (
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
    ,case
       when tactic like 'Google Search%' then 'Search Google'
       when tactic like 'Bing Search%' then 'Search Bing'
       when tactic like 'Google Shopping%' then 'Shopping Google'
       when connection = 'Sprout' then 'Sprout'
       when engine = 'Facebook' then 'Facebook'
       when engine = 'Instacart' then 'Instacart'
       when engine = 'Target' then 'Target'
       when engine = 'Attentive' then 'Attentive'
       when engine = 'Walmart' then 'Walmart'
       when engine = 'Klaviyo' then 'Klaviyo'
       when engine = 'AmazonDSP' then 'Amazon DSP'
       when (engine like '%Amazon%' and engine != 'AmazonDSP') then 'Amazon Search'
       when (engine = 'Google' and channel = 'Programmatic') then 'YouTube'
       else engine
     end as tactic_pacing
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

    ,sum(conversions_platform) as conversions_platform
    ,sum(impressions) as impressions
    ,sum(clicks) as clicks
    ,sum(link_clicks) as link_clicks
    ,sum(engagement) as engagement
    ,sum(comments) as comments
    ,sum(sessions) as sessions
    ,sum(spend) as spend
    ,sum(conv_ntb) as conv_ntb
    ,sum(rev_ntb) as rev_ntb
    ,sum(new_user_sessions) as new_user_sessions
    ,sum(bounces) as bounces
    ,sum(revenue_platform) as revenue_platform
    ,sum(conversions_ntb) as conversions_ntb
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

    from source
    {{ dbt_utils.group_by(20) }}

)

, lod as (
  select 
    date
    ,coalesce(device, 'Unknown') as device
    ,case
       when len(tactic)=0 or tactic is null then 'Unknown'
       else tactic
     end as tactic
    ,case
       when len(product_category)=0 or product_category is null then 'Unknown'
       else product_category
     end as product_category
    ,coalesce(engine, 'Unknown') as engine
    ,coalesce(channel, 'Unknown') as channel
    ,coalesce(campaign_branded, 'Unknown') as campaign_branded
    ,connection
    ,_dw_connection
    ,coalesce(tactic_pacing, 'Unknown') as tactic_pacing
    ,case
       when len(utm_content) = 0 or utm_content is null then 'Unknown'
       else utm_content
     end as utm_content
    ,case
       when len(campaign)=0 or campaign is null then 'Unknown'
       else campaign
     end as campaign
    ,case
       when len(product)=0 or product is null then 'Unknown'
       else product
     end as product
    ,insert_date
    ,coalesce(ad_group, 'Unknown') as ad_group
    ,coalesce(utm_campaign, 'Unknown') as utm_campaign
    ,coalesce(utm_medium, 'Unknown') as utm_medium
    ,coalesce(utm_source, 'Unknown') as utm_source
    ,coalesce(ad_creative, 'Unknown') as ad_creative
    ,coalesce(state, 'Unknown') as state
    , case
       when len(ad)=0 or ad is null then 'Unknown'
       else ad
     end as ad

    , conversions_platform
    ,impressions
    ,clicks
    ,link_clicks
    ,engagement
    ,comments
    ,sessions
    ,bounces
    ,revenue_platform
    ,conversions_ntb
    ,revenue_ntb
    ,conversions_ga
    ,spend
    ,revenue_ga
    ,unique_purchases
    ,product_revenue
    ,item_quantity
    ,new_user_sessions
    ,conv_ntb
    ,rev_ntb
    ,pageviews
    ,video_views
    ,eligible_impressions
    ,impressions_for_calc_use_only
    ,sum(sessions) over (partition by engine) as lod_ga
    ,sum(conversions_platform) over (partition by connection) as lod_conv
    ,sum(revenue_platform) over (partition by connection) as lod_rev

    from transformed_level1
    )

, transformed_level2 as (
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
    ,tactic_pacing
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
    ,insert_date

    ,conversions_platform
    ,impressions
    ,clicks
    ,link_clicks
    ,engagement
    ,comments
    ,sessions
    ,spend
    ,new_user_sessions
    ,conv_ntb
    ,rev_ntb
    ,bounces
    ,revenue_platform
    ,conversions_ntb
    ,revenue_ntb
    ,conversions_ga
    ,revenue_ga
    ,unique_purchases
    ,product_revenue
    ,item_quantity
    ,pageviews
    ,video_views
    ,eligible_impressions
    ,impressions_for_calc_use_only
    ,lod_ga
    ,lod_conv
    ,lod_rev
    ,spend*1000 as spend1000
    ,case
       when lod_ga > 0 then spend
     end as spend_ga
    ,case
       when lod_conv > 0 then spend
     end as spend_conv
    ,case
       when lod_ga > 0 then clicks
     end as clicks_ga
    ,case
       when lod_conv > 0 then clicks
     end as clicks_conv
    ,case
       when lod_rev > 0 then spend
     end as spend_rev

    from lod
    )

, final_aggregation as (
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
    ,tactic_pacing
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

    ,sum(conversions_platform) as conversions_platform
    ,sum(impressions) as impressions
    ,sum(clicks) as clicks
    ,sum(link_clicks) as link_clicks
    ,sum(engagement) as engagement
    ,sum(new_user_sessions) as new_user_sessions
    ,sum(comments) as comments
    ,sum(sessions) as sessions
    ,sum(bounces) as bounces
    ,sum(revenue_platform) as revenue_platform
    ,sum(conversions_ntb) as conversions_ntb
    ,sum(rev_ntb) as rev_ntb
    ,sum(conv_ntb) as conv_ntb
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
    ,sum(spend1000) as spend1000
    ,sum(spend_ga) as spend_ga
    ,sum(spend_conv) as spend_conv
    ,sum(clicks_ga) as clicks_ga
    ,sum(clicks_conv) as clicks_conv
    ,sum(spend_rev) as spend_rev
    ,sum(spend) as spend

    from transformed_level2
     {{ dbt_utils.group_by(20) }}
  )

select *

from final_aggregation