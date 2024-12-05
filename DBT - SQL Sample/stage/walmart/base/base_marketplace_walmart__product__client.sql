{{ config(
    tags=['client', 'base', 'walmart']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'bigquery__product') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date  
      ,campaign_name::varchar
      ,adgroup_name::varchar
      ,product_title::varchar
      ,'v_bigquery_product_ads_report'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,impressions::integer
      ,clicks::integer
      ,cost::numeric(14,7)
      ,attributedunitsordered14d::integer
      ,attributedsales14d::numeric(14,7)

    from source
    where report_date < current_date
        and brand_name = 'Welly'
        and channel_type = 'walmart_seller'
  )

select *
from filtered