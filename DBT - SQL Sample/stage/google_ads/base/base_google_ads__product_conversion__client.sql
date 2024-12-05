{{ config(
    tags=['client', 'base', 'google_ads']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'google_ads__product_conversion') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date
      ,campaign::varchar
      ,ad_group::varchar
      ,device::varchar
      ,product_title::varchar
      ,'v_google_ads_api_product_conversion_tinuiti'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,conversions::numeric(14,7)
      ,conversion_value::numeric(14,7)

    from source
    where report_date < current_date
  )
  
select *
from filtered