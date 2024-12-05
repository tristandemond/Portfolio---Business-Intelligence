{{ config(
      tags=['client', 'stage', 'google_ads']
) }}

with
  source1 as (
    select *
    from {{ ref('base_google_ads__ad_group__client') }}
  )

  ,source2 as (
    select *
    from {{ ref('base_google_ads__campaign__client') }}
  )

  ,search_filtered as (
    select
      distinct campaign_id
      , campaign_type

    from source2
    where lower(campaign_type) != 'shopping'
  )

  ,ads as (
    select 
      a.report_date
      ,a.campaign
      ,a._dw_connection
      ,a.campaign_id
      ,a.ad_group
      ,a.device
      ,max(a.insert_date) as insert_date
      
      ,sum(case
              when a.search_impression_share is null then 0
              else a.impressions
           end) as impressions_for_calc_use_only
      ,sum((case
              when a.search_impression_share is null then 0
              else a.impressions
           end)/search_impression_share) as eligible_impressions
      ,sum(a.impressions) as impressions
      ,sum(a.cost) as spend
      ,sum(a.clicks) as clicks
      ,sum(a.conversions) as conversions_platform
      ,sum(a.conversion_value) as revenue_platform
      
    from source1 a
    inner join search_filtered b 
    on a.campaign_id = b.campaign_id
    {{ dbt_utils.group_by(6) }}

  )    

  , transformed_level1 as (
    select
      report_date as date
      , campaign
      , campaign_id
      , ad_group
      , _dw_connection
      , device
      , 'Google Ads Search'::varchar as connection
      , insert_date

      , impressions
      , spend
      , clicks
      , conversions_platform
      , revenue_platform
      , eligible_impressions::numeric(14,7)
      , impressions_for_calc_use_only::numeric(14,7)

      from ads
  )

select *
from transformed_level1

