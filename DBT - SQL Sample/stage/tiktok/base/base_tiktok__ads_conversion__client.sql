{{ config(
    tags=['client', 'base', 'social', 'tiktok']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'tiktok__ad_conversion') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date 
      ,campaign_name::varchar  
      ,ad_group_name::varchar 
      ,ad_name::varchar
      ,ad_text::varchar as ad_creative
      ,conversion_name
      ,'v_tiktok_ad_standard_conversion'::varchar as _dw_connection
      ,insert_date::date as insert_date
      
      ,conversions::integer
      ,conversion_value::numeric(14,7)

    from source
    where report_date < current_date
  )
  
select *
from filtered