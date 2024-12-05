{{ config(
      tags=['welly', 'stage', 'bing']
) }}

with
  source as (
    select *
    from {{ ref('base_bing__ads_performance__welly') }}
  )

  ,transformed_level1 as (
    select
    report_date as date
    , 'Search'::varchar as channel
    , 'Bing'::varchar as engine
    , 'v_bing_ads'::varchar as connection
    , _dw_connection
    , campaign
    , ad_group
    , ad_id as ad_creative
    , case
        when device = 'Mobile Phone' then 'Mobile'
        when device = 'Smartphone' then 'Mobile'
        when device = 'Tablet' then device
        when device = 'Computer' then 'Desktop'
        else 'Unknown'
      end as device
    , case
        when lower(campaign) like '%monster%' then 'Bravery Bandages Monster'
        when lower(campaign) like '%pets%' then 'Bravery Bandages Peculiar Pets'
        when lower(campaign) like '%eva chen%' then 'Welly x Eva Chen'
        when lower(campaign) like '%bravery bandages tins%' then 'Bravery Bandages Tins'
        when lower(campaign) like '%bravery%' then 'Bravery Bandages'
        when (lower(campaign) like '%hydrocolloid%' or lower(campaign) like '%face saver%') then 'Hydrocolloid Face Savers'
        when lower(campaign) like '%refill%' then 'Refills'
        when lower(campaign) like '%ointment%' then 'Ointments'
        when lower(campaign) like '%kit%' then 'Kits'
        when lower(campaign) like '%bandage%' then 'Other Bandages'
        when lower(campaign) like '%bundle%' then 'Bundles'
        else 'Unknown'
      end as product
    , case
        when lower(product) like '%hydrocolloid%' then 'Hydrocolloid'
        when lower(product) like '%refills%' then 'Refills'
        when lower(product) like '%kit%' then 'Kits'
        when lower(product) like '%bandages%' then 'Bandages'
        when lower(product) like '%bundles%' then 'Bundles'
        else 'Unknown'
      end as product_category
    , max(insert_date) as insert_date

    , sum(impressions) as impressions
    , sum(clicks) as clicks
    , sum(cost) as cost
    , sum(conversions) as conversions_platform
    , sum(revenue) as revenue_platform

    from source 
    {{ dbt_utils.group_by(11) }}
  )

  , transformed_level2 as (
    select
      date
      , channel
      , engine
      , connection
      , campaign
      ,_dw_connection
      , case
          when lower(campaign) like '%smart shopping%' then engine + ' Shopping Smart'
          when lower(campaign) like '%holiday shopping%' then engine + ' Holidy Shopping'
          when lower(campaign) like '%_nonbrand%' then engine + ' ' + channel + ' Nonbrand'
          when (lower(campaign) like '%brand terms%' or lower(campaign) like '%_brand%') then engine + ' ' + channel + ' Brand'
          when (lower(campaign) like '%display%' or lower(campaign) like '%youtube%') then engine + ' ' + 'Youtube'
          else engine + ' ' + channel + ' Nonbrand'
        end as tactic
      , ad_group
      , ad_creative
      , device
      , product
      , product_category
      , insert_date

      , impressions
      , clicks
      , cost
      , conversions_platform
      , revenue_platform

    from transformed_level1
  )

, transformed_level3 as (
    select
      date
      ,channel
      ,engine
      ,connection
      ,campaign
      ,_dw_connection
      ,case
         when lower(tactic) like '%nonbrand%' then 'Nonbrand'
         when lower(tactic) like '%brand%' then 'Brand'
         else 'Unknown'
       end as campaign_branded
      ,ad_group
      ,ad_creative
      ,device
      ,product
      ,tactic
      ,product_category
      ,insert_date

      ,impressions
      ,clicks
      ,cost as spend
      ,conversions_platform
      ,revenue_platform

    from transformed_level2
  )

select *

from transformed_level3