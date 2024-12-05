{{ config(
    tags=['client', 'base', 'bigquery', 'amazon']
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
      ,campaigntype::varchar
      ,targetingtype::varchar
      ,channel_type::varchar
      ,'v_bigquery_campaigns_report'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,impressions::integer
      ,clicks::integer
      ,cost::numeric(14,7)
      ,attributeddetailpageviewsclicks14d::integer
      ,attributedconversions14d::integer
      ,attributedsales14d::numeric(14,7)
      ,attributedordersnewtobrand14d::integer
      ,attributedsalesnewtobrand14d::numeric(14,7)
      
    from source
    where brand_name = 'client'
        and (channel_id = '3729' 
        or channel_id = '3775')
        and report_date >= '2021-11-01'
  )

select *
from filtered