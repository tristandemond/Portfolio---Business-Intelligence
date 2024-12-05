{{ config(
    tags=['client', 'base', 'google_ads']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'google_ads__ad_group') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date  
      ,campaign::varchar
      ,campaign_id::varchar
      ,ad_group::varchar
      ,device::varchar
      ,'v_google_ads_api_ad_group_tinuiti'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,impressions::integer
      ,search_impression_share::numeric(14,7)
      ,cost::numeric(14,7)
      ,clicks::integer
      ,conversions::numeric(14,7)
      ,conversion_value::numeric(14,7)
      
    from source
    where report_date < current_date
  )
  
select *
from filtered