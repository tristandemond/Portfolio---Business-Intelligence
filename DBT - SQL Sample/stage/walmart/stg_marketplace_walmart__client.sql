{{ config(
      tags=['welly', 'stage', 'walmart']
) }}

with
  source as (
    select *
    from {{ ref('base_marketplace_walmart__product__welly') }}
  )

  ,aggregated as (
      select
        report_date as date
        ,campaign_name as campaign
        ,split_part(campaign,'_',2)::varchar as product_category
        ,'Marketplaces'::varchar as channel
        ,'Walmart'::varchar as engine
        ,'Walmart'::varchar as connection
        ,adgroup_name as ad_group
        ,product_title as product
        ,_dw_connection
        ,max(insert_date)

        ,sum(impressions) as impressions
        ,sum(clicks) as clicks
        ,sum(cost) as spend
        ,sum(attributedunitsordered14d) as conversions_platform
        ,sum(attributedunitsordered14d) as conversions_ga
        ,sum(attributedsales14d) as revenue_platform
        ,sum(attributedsales14d) as revenue_ga

      from source 
      {{ dbt_utils.group_by(9) }}
  )

select *

from aggregated