{{ config(
      tags=['client', 'stage', 'google_ads']
) }}

with
  unioned as (
    {{ dbt_utils.union_relations(
        relations=[
          ref('stg_google_ads_search__client')
          , ref('stg_google_ads_shopping__client')
          ]
    ) }}
  )

  , aggregated as (
      select
        date
        ,campaign
        ,campaign_id
        ,ad_group
        ,'Google'::varchar as engine
        ,case
            when connection = 'Google Ads Search' then
              case
                when lower(campaign) like '%display%' then 'Programmatic'
                else 'Search'
              end
            when connection = 'Google Ads Shopping' then 'Shopping'            
         end as channel
        ,case
            when device = 'MOBILE' then 'Mobile'
            when device = 'TABLET' then 'Tablet'
            when device = 'Computer' then 'Desktop'
            when device = 'DESKTOP' then 'Desktop'
            else 'Unknown'
         end as device
        , product
        ,case
            when lower(product) like '%bandages%' then 'Bandages'
            when lower(product) like '%bandage%' then 'Bandages'
            when lower(product) like '%kit%' then 'Kits'
            when lower(product) like '%bundle%' then 'Bundles'
            when lower(product) like '%hydrocolloid%' then 'Hydrocolloid'
            when lower(product) like '%bandages%' then 'Hydrocolloid'
            when lower(product) like '%refill%' then 'Refill'
         end as product_category
        ,connection
        ,_dw_connection
        ,max(insert_date) as insert_date
        
        ,sum(impressions) as impressions 
        ,sum(spend) as spend
        ,sum(clicks) as clicks
        ,sum(conversions_platform) as conversions_platform
        ,sum(revenue_platform) as revenue_platform
        ,sum(eligible_impressions) as eligible_impressions
        ,sum(impressions_for_calc_use_only) as impressions_for_calc_use_only

      from unioned
      {{ dbt_utils.group_by(11) }}  
  )

  , filtered as (
      select
        date
        , campaign
        , campaign_id
        , ad_group
        , channel
        , device
        , product
        , product_category
        , case
            when connection = 'Google Ads Shopping' then
              case
                when lower(campaign) like '%smart shopping%'  then engine || ' Shopping Smart'
                when lower(campaign) like '%holiday shopping%' then engine || 'Holiday Shopping'
                when (lower(campaign) like '%brand terms%' or lower(campaign) like '%\\_brand%') then engine || ' ' || channel || ' Brand'
                when (lower(campaign) like '%display%' or lower(campaign) like '%youtube%') then engine || ' YouTube'
                when (lower(campaign) like '%pmax%' and lower(campaign) like '%\\_brand%') then engine || ' PMax Brand'
                when (lower(campaign) like '%pmax%' and lower(campaign) like '%\\_nonbrand%') then engine || ' PMax Nonbrand'
                else engine || ' ' || channel || ' Nonbrand'
              end
            when connection = 'Google Ads Search' then
              case
                when lower(campaign) like '%smart shopping%' then engine || ' Shopping Smart'
                when lower(campaign) like '%holiday shopping%' then engine || ' Holiday Shopping'
                when (lower(campaign) like '%brand terms%' or lower(campaign) like '%\\_brand%') then engine || ' ' || channel || ' Brand'
                when (lower(campaign) like '%display%' or lower(campaign) like '%youtube%') then engine || ' YouTube'
                else engine || ' ' || channel || ' Nonbrand'
              end
            end as tactic
        , case
            when lower(tactic) like '%nonbrand%' then 'Nonbrand'
            when lower(tactic) like '%brand%' then 'Brand'
            else 'Unknown'
          end as campaign_branded
        , connection
        , _dw_connection
        , engine
        , insert_date
        
        , impressions 
        , spend
        , clicks
        , conversions_platform
        , revenue_platform
        , eligible_impressions
        , impressions_for_calc_use_only

      from aggregated 
  )

select *
from filtered