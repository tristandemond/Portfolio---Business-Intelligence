{{ config(
    tags=['client', 'base', 'google_ads']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'google_ads__campaign') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date
      ,device::varchar
      ,campaign::varchar
      ,campaign_id::varchar
      ,campaign_type::varchar
      ,'v_google_ads_api_campaign_tinuiti'::varchar as _dw_connection
      ,insert_date::date as insert_date
      
      ,impressions::integer
      ,clicks::integer
      ,cost::numeric(14,7)
      ,conversion_value::numeric(14,7)
      ,conversions::numeric(14,7)
      
    from source
    where report_date < current_date
      and (lower(campaign) like '%pmax%' or lower(campaign_type) != 'shopping')
  )
  
select *
from filtered