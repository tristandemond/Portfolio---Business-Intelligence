{{ config(
    tags=['client', 'base', 'social', 'tiktok']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'tiktok__ad_region') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date 
      ,campaign_name::varchar  
      ,ad_group_name::varchar 
      ,ad_name::varchar
      ,ad_text::varchar as ad_creative
      ,region::varchar
      ,'v_tiktok_ad_geo_region_standard'::varchar as _dw_connection
      ,insert_date::date as insert_date
      

    
    from source
    where report_date < current_date
  )
  
select *
from filtered