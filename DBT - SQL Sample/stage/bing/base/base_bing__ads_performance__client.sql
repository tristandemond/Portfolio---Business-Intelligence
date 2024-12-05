{{ config(
    tags=['client', 'base', 'bing']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'bing__ad_performance') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date
      ,campaign::varchar
      ,ad_group::varchar
      ,device::varchar
      ,ad_id::varchar
      ,'v_bing_ads_ad_performance_tinuiti'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,impressions::integer
      ,clicks::integer
      ,cost::numeric(14,7)
      ,conversions::integer
      ,revenue::numeric(14,7)

    from source
    where  report_date < current_date
  )

select *
from filtered