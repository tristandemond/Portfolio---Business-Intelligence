{{ config(
    tags=['client', 'base', 'spotify']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'spotify__dai_span') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date
      ,campaign::varchar as campaign
      ,line_item::varchar as ad_group
      ,creative::varchar as ad_creative
      ,'v_upload_spotify_dai_span'::varchar as _dw_connection
      ,insert_date::date as insert_date
      
      ,ad_server_impressions::integer as impressions
      ,spend::numeric(14,7) as spend
      ,ad_server_clicks::integer as clicks

    from source
    where report_date < current_date
  )
  
select *
from filtered