{{ config(
    tags=['client', 'base', 'ga']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'ga__session') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date as date 
      ,campaign::varchar
      ,adgroup::varchar
      ,source::varchar
      ,medium::varchar
      ,adcontent::varchar 
      ,usertype::varchar
      ,device::varchar
      ,'Google Analytics Ad'::varchar as connection
      ,'v_ga_session'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,sessions::integer
      ,bounces::integer
      ,pageviews::integer
      ,transactions::integer as conversions_ga
      ,transactionrevenue::numeric(14,7) as revenue_ga
      
    from source
    where report_date < current_date
  )
  
select *
from filtered