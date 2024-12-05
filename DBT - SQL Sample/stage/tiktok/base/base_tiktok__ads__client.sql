{{ config(
    tags=['client', 'base', 'social', 'tiktok']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'tiktok__ad') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date 
      ,campaign_name::varchar  
      ,ad_group_name::varchar 
      ,ad_name::varchar
      ,ad_text::varchar as ad_creative
      ,'v_tiktok_ad_standard'::varchar as _dw_connection
      ,insert_date::date as insert_date
      
      ,cost::numeric(14,7)
      ,video_views::integer
      ,impression::integer
      ,click::integer
    
    from source
    where report_date < current_date
  )
  
select *
from filtered