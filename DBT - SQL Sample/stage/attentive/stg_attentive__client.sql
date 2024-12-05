{{ config(
      tags=['client', 'stage', 'sizmek']
) }}

with
  source as (
    select *
    from {{ ref('base_attentive__sms__client') }}
  )

  ,aggregated as (
    select
      day as date
      ,'Attentive SMS'::varchar as connection
      ,_dw_connection
      ,'SMS'::varchar as channel
      ,'Attentive'::varchar as engine
      ,'Mobile'::varchar as device
      ,'Brand'::varchar as campaign_branded 
      ,message_name as campaign
      ,max(insert_date) as insert_date

      ,sum(delivered) as impressions
      ,sum(clicks) as clicks
      ,sum(conversions) as conversions_platform
      ,sum(conversions) as conversions_ga
      ,sum(revenue) as revenue_platform
      ,sum(revenue) as revenue_ga

    from source
    {{ dbt_utils.group_by(8) }}

  )

select *

from aggregated