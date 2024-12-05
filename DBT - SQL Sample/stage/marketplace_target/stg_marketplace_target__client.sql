{{ config(
      tags=['client', 'stage', 'target']
) }}

with
  source as (
    select *
    from {{ ref('base_marketplace_target__campaign__client') }}
  )

  ,aggregated as (
      select
        report_date as date
        ,'Target'::varchar as connection
        ,_dw_connection
        ,'Marketplaces'::varchar as channel
        ,'Target'::varchar as engine
        ,campaign
        ,max(insert_date) as insert_date

        ,sum(impressions) as impressions
        ,sum(clicks) as clicks
        ,sum(spend) as spend
        ,sum(units) as units
        ,sum(units) as conversions_platform
        ,sum(units) as conversions_ga
        ,sum(sales) as revenue_platform
        ,sum(sales) as sales
        ,sum(sales) as revenue_ga
  
      from source
      {{ dbt_utils.group_by(6) }}
  )

select *

from aggregated