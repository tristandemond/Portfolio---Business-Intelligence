{{ config(
    tags=['client', 'social', 'base', 'facebook']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'facebook__ad_standard_conversion') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date
      ,campaign::varchar
      ,ad_set::varchar
      ,ad::varchar
      ,'v_facebook_ad_standard_conversion'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,conversions::numeric(14,7)
      ,conversion_value::numeric(14,7)

    from source
    where  report_date < current_date
      and conversion_name = 'offsite_conversion.fb_pixel_purchase'
  )

select *
from filtered