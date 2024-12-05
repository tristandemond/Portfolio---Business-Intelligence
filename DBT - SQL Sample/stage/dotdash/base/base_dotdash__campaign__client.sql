{{ config(
    tags=['client', 'base', 'dotdash']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'first_aid__campaign') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date
      ,"order"::varchar as campaign
      ,creative::varchar as ad
      ,'v_upload_first_aid_campaign'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,ad_server_impressions::integer as impressions
      ,ad_server_clicks::integer as clicks
      ,ad_server_revenue::numeric(14,7) as revenue_platform

    from source
  )
  
select *
from filtered
