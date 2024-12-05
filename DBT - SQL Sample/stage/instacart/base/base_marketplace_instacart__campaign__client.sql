{{ config(
    tags=['client', 'base', 'instacart']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'bigquery__campaigns') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date  
      ,campaign_name::varchar
      ,'v_bigquery_campaigns_report'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,attributeddetailpageviewsclicks14d::integer
      ,attributedconversions14d::integer
      ,attributedsales14d::numeric(14,7)
      ,attributedordersnewtobrand14d::integer
      ,attributedsalesnewtobrand14d::numeric(14,7)
      ,impressions::integer
      ,clicks::integer
      ,cost::numeric(14,7)
      
    from source
    where report_date < current_date
        and brand_name = 'client'
        and channel_type = 'instacart'
        and channel_id = '3764'
  )
  
select *
from filtered