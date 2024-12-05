{{ config(
    tags=['client', 'base', 'ttd']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'ttd__reports') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date  
      ,campaign::varchar
      ,ad_group::varchar
      ,'v_ttd_my_reports_basic_performance'::varchar as _dw_connection
      ,creative::varchar
      ,insert_date::date as insert_date

      ,partner_cost_adv_currency::numeric(14,7)
      ,advertiser_cost_adv_currency::numeric(14,7)

    from source
  )
  
select *
from filtered