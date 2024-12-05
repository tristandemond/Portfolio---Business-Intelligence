{{ config(
    tags=['client', 'base', 'sprout']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'sprout__daily') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date  
      ,profile::varchar
      ,'v_upload_sprout_daily'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,impressions::integer
      ,post_link_clicks::integer
      ,other_post_clicks::integer
      ,video_views::integer
      ,engagements::integer
      ,comments::integer
      
    from source
    where report_date < current_date
  )
  
select *
from filtered