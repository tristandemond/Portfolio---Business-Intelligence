{{ config(
    tags=['client', 'base', 'social', 'facebook']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'facebook__ad') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date
      ,campaign::varchar
      ,ad_set::varchar
      ,ad::varchar
      ,device_platform::varchar
      ,'v_facebook_ad_standard'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,impressions::integer 
      ,link_clicks::integer as clicks
      ,cost::numeric(14,7)

    from source
    where  report_date < current_date
  )

select *
from filtered