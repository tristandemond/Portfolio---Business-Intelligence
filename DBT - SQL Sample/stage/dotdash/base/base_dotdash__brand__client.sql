{{ config(
    tags=['client', 'base', 'dotdash']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'custom__brand_pageviews') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date
      ,custom_content::varchar as campaign
      ,'v_upload_custom_brand_pageviews'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,pageviews_custom_brand_review::integer as pageviews
      ,spend_custom_brand_review::numeric(14,7)
      ,spend_personalized_prepared_quiz::numeric(14,7)
      ,spend_road_trip_essentials::numeric(14,7)

    from source
  )
  
select *
from filtered
