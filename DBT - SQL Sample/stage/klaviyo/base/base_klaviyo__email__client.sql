{{ config(
    tags=['client','klaviyo', 'base', 'email']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'klaviyo__email') }}
    where client = 'client'
  )

  ,filtered as (
    select
      campaign_date::date as date 
      ,campaign_name::varchar
      ,'core.email'::varchar as _dw_connection

      ,deliveries::integer
      ,clicks::integer
      ,conversions::integer
      ,bounces::integer

    from source
  )
  
select *
from filtered
