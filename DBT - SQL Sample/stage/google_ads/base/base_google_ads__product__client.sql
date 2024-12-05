{{ config(
    tags=['client', 'base', 'google_ads']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'google_ads__product') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date
      ,campaign_name::varchar as campaign
      ,ad_group_name::varchar as ad_group
      ,device::varchar
      ,'v_google_ads_api_product'::varchar as _dw_connection
      ,product_title::varchar
      ,insert_date::date as insert_date
      
      ,impressions::integer
      ,cost::numeric(14,7)
      ,clicks::integer

    from source
    where report_date < current_date
  )
  
select *
from filtered