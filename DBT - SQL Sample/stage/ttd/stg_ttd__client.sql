{{ config(
      tags=['client', 'stage', 'ttd']
) }}

with
  source as (
    select *
    from {{ ref('base_ttd__performance__client') }}
  )

  ,aggregated as (
    select
      report_date as date
      ,'TTD'::varchar as connection
      ,_dw_connection
      ,'Programmatic'::varchar as channel
      ,'TTD'::varchar as engine
      ,'Brand'::varchar as campaign_branded
      ,creative as ad_creative
      ,ad_group
      ,campaign
      ,split_part(campaign,'_',6) as tactic
      ,max(insert_date) as insert_date

      ,sum(partner_cost_adv_currency) as spend
      ,sum(advertiser_cost_adv_currency) as revenue_platform

    from source  
    {{ dbt_utils.group_by(10) }}
  )

select *

from aggregated