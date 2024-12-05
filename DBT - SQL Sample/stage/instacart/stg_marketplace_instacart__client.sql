{{ config(
      tags=['client', 'stage', 'instacart']
) }}

with
  source as (
    select *
    from {{ ref('base_marketplace_instacart__campaign__welly') }}
  )

  ,aggregated as (
      select
        report_date as date
        ,campaign_name as campaign
        ,case
           when lower(campaign) like '%monster%' then 'Bravery Bandages Monster'
           when lower(campaign) like '%pets%' then 'Bravery Bandages Peculiar Pets'
           when lower(campaign) like '%eva chen%' then 'Welly x Eva Chen'
           when lower(campaign) like '%tins%' then 'Bravery Bandages Tins'
           when lower(campaign) like '%bravery%' then 'Bravery Bandages'
           when lower(campaign) like '%hydrocolloid%' then 'Hydrocolloid Face Savers'
           when lower(campaign) like '%face saver%' then 'Hydrocolloid Face Savers'
           when lower(campaign) like '%refill%' then 'Refills'
           when lower(campaign) like '%ointment%' then 'Ointments'
           when lower(campaign) like '%kit%' then 'Kits'
           when lower(campaign) like '%bandage%' then 'Other Bandages'
           when lower(campaign) like '%bundle%' then 'Bundles'
           else 'Unknown'
         end as product
        ,case
           when lower(campaign) like '%bandages%' then 'Bandages'
           when lower(campaign) like '%kit%' then 'Kits'
           when lower(campaign) like '%bundle%' then 'Bundles'
           when lower(campaign) like '%hydrocolloid%' then 'Hydrocolloids'
           when lower(campaign) like '%refill%' then 'Refills'
           else 'Unknown'
         end as product_category
        ,'Instacart'::varchar as tactic
        ,'Instacart'::varchar as engine
        ,'Marketplaces'::varchar as channel
        ,'Instacart'::varchar as connection
        ,_dw_connection
        ,max(insert_date)
      
        ,sum(attributeddetailpageviewsclicks14d) as pageviews
        ,sum(attributedconversions14d) as conversions_platform
        ,sum(attributedconversions14d) as conversions_ga
        ,sum(attributedsales14d) as revenue_platform
        ,sum(attributedsales14d) as revenue_ga
        ,sum(attributedordersnewtobrand14d) as converstions_ntb
        ,sum(attributedsalesnewtobrand14d) as revenue_ntb
        ,sum(impressions) as impressions
        ,sum(clicks) as clicks
        ,sum(cost) as spend

      from source
      {{ dbt_utils.group_by(9) }}
  )

select *

from aggregated