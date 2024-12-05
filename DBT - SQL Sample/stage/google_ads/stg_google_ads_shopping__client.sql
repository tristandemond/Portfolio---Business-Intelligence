{{ config(
      tags=['client', 'stage', 'google_ads']
) }}

with
  unioned as (
    {{ dbt_utils.union_relations(
        relations=[
          ref('base_google_ads__product__client')
          , ref('base_google_ads__campaign__client')
          , ref('base_google_ads__product_conversion__client')
          ]
    ) }}
  )

  ,aggregated as (
      select
        report_date as date
        ,campaign
        ,ad_group
        ,_dw_connection
        ,device
        ,product_title as product
        ,'Google Ads Shopping'::varchar as connection
        ,case
           when _dw_connection = 'v_google_ads_api_campaign_tinuiti' and lower(campaign) like '%pmax%' then true
           when _dw_connection in ('v_google_ads_api_product','v_google_ads_api_product_conversion_tinuiti') then true
           else false
         end as _dw_filter
        ,max(insert_date)
        
        ,sum(impressions) as impressions 
        ,sum(clicks) as clicks
        ,sum(cost) as spend
        ,sum(conversion_value) as revenue_platform
        ,sum(conversions) as conversions_platform   

      from unioned  
      where _dw_filter = true
      {{ dbt_utils.group_by(7) }}
      
  )

select *

from aggregated