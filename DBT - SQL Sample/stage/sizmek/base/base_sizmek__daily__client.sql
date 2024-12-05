{{ config(
    tags=['client', 'base', 'sizmek']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'sizmek__reporting_daily') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date
      ,ad_name::varchar as ad
      ,placement_name::varchar as ad_creative
      ,site_name::varchar
      ,campaign_name::varchar
      ,'v_upload_sizmek_reporting_daily'::varchar as _dw_connection
      ,insert_date::date as insert_date
      
      ,impressions_net::numeric(14,7)
      ,clicks_net::numeric(14,7)

    from source
    where report_date < current_date
  )
  
select *
from filtered