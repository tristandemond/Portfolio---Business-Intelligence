{{ config(
    tags=['client', 'social', 'base', 'facebook']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'facebook__ad_region') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date
      ,campaign::varchar
      ,ad_set::varchar
      ,ad::varchar
      ,region::varchar
      ,'v_facebook_ad_region'::varchar as _dw_connection
      ,insert_date::date as insert_date

    from source
    where  report_date < current_date
  )

select *
from filtered