{{ config(
      tags=['welly', 'stage', 'facebook', 'social']
) }}

with
  unioned as (
    {{ dbt_utils.union_relations(
        relations=[
          ref('base_facebook__ads__welly')
          , ref('base_facebook__ads_conversion__welly')
          , ref('base_facebook__ads_region__welly')
          ]
    ) }}
  )

  ,transformed_level1 as (
    select
      report_date as date
      , campaign
      , ad_set as ad_group
      , ad
      , ad as ad_creative
      , region as state
      , _dw_connection
      , case
          when device_platform = 'desktop' then 'Desktop'
          when device_platform = 'mobile_app' then 'Mobile'
          when device_platform = 'mobile_web' then 'Mobile'
          when device_platform = 'unknown' then 'Unknown'
          else device_platform
        end as device
      , 'v_facebook_ads'::varchar as connection
      , 'Facebook'::varchar as engine
      , 'Social'::varchar as channel
      , max(insert_date) as insert_date

      , sum(impressions) as impressions
      , sum(clicks) as clicks
      , sum(cost) as cost
      , sum(conversion_value) as conversion_value
      , sum(conversions) as conversions  

    from unioned  
    {{ dbt_utils.group_by(10) }}
  )

  ,transformed_level2 as (
    select 
      date
      , campaign
      , ad_group
      , ad
      , ad_creative
      , state
      , _dw_connection
      , device
      , connection
      , engine
      , channel
      , case
          when lower(campaign) like '%retargeting%' then engine || ' Retargeting'
          when lower(campaign) like '%prospecting%' then engine || ' Prospecting'
          when lower(campaign) like '%retention%' then engine || ' Retention'
          else engine || ' Unknown'
        end as tactic
      , insert_date

      , impressions
      , clicks
      , cost
      , conversion_value
      , conversions  
    
    from transformed_level1
  )

  ,transformed_level3 as (
    select 
      date
      ,campaign
      ,ad_group
      ,ad
      ,ad_creative
      ,state
      ,_dw_connection
      ,device
      ,connection
      ,engine
      ,case
         when lower(ad) like '%bandages' then 'Bandages'
         when lower(ad) like '%kit%' then 'Kits'
         when lower(ad) like '%kits%' then 'Kits'
         when lower(ad) like '%bundle%' then 'Bundles'
         when lower(ad) like '%hydrocolloid%' then 'Hydrocolloids'
         when lower(ad) like '%refill%' then 'Refills'
         else 'Unknown'
       end as product_category
      ,channel
      ,tactic
      ,case
         when lower(tactic) like '%nonbrand%' then 'Nonbrand'
         when lower(tactic) like '%brand%' then 'Brand'
         else 'Unknown'
       end as campaign_branded
      ,  insert_date

      ,impressions
      ,clicks
      ,cost as spend
      ,conversion_value as revenue_platform
      ,conversions as conversions_platform

    from transformed_level2
  )

select *

from transformed_level3