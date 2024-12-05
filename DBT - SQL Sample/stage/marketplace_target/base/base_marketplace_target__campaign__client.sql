{{ config(
    tags=['client', 'base', 'target']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'target__campaign') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date  
      ,campaign::varchar
      ,'v_upload_target_campaign_level_daily'::varchar as _dw_connection
      ,insert_date:: date

      ,impressions::integer
      ,clicks::integer
      ,spend::numeric(14,7)
      ,units::integer
      ,sales::numeric(14,7)
      
    from source
    where report_date < current_date
  )
  
select *
from filtered