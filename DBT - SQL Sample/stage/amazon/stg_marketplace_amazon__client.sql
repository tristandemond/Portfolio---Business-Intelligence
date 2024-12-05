{{ config(
      tags=['client', 'stage', 'amazon', 'bigquery']
) }}

with
  source as (
    select *
    from {{ ref('base_marketplace_amazon__campaign_client') }}
  )

  ,aggregated as (
    select
      report_date as date
      , campaign_name as campaign
      , targetingtype
      , case
          when lower(campaign) like '%monster%' then 'Bravery Bandages Monster'
          when lower(campaign) like '%pets%' then 'Bravery Bandages Peculiar Pets'
          when lower(campaign) like '%eva chen%' then 'client x Eva Chen'
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
      , case
          when lower(campaign) like '%hydrocolloid%' then 'Hydrocolloids'
          when lower(campaign) like '%refill%' then 'Refills'
          when lower(campaign) like '%kit%' then 'Kits'
          when lower(campaign) like '%kits%' then 'Kits'
          when lower(campaign) like '%bandages%' then 'Bandages'
          when lower(campaign) like '%bundle%' then 'Bundles'
          else 'Unknown'
        end as product_category
      , case
          when (campaign = 'client_Display_US_Amazon_Awareness_OTT_AmazonFireTV_FirstAid' or lower(campaign) like '%br |%') then 'Amazon' || targetingtype || 'Brand'
          when (lower(campaign) like '%pt |%' or lower(campaign) like '%consideration%') then 'Amazon' || targetingtype || 'Consideration'
          when (lower(campaign) like '%mtnb |%' or lower(campaign) like '%sbvnb |%' or lower(campaign) like '%sbnb |%' or lower(campaign) like '%mtmw |%' or lower(campaign) like '%sbvmw |%' or lower(campaign) like '%sbmw |%') then 'Amazon' || targetingtype || 'Awareness'
          when lower(campaign) like '%nb |%' then 'Amazon' || targetingtype || 'Nonbrand'
          else 'Amazon' || targetingtype || 'Unknown'
        end as tactic    
      , case
          when channel_type = 'dsp' then 'Amazon' || 'DSP'
          else 'Amazon' || campaigntype
        end as engine
      , 'Marketplaces'::varchar as channel
      , 'Amazon'::varchar as connection
      , _dw_connection
      , max(insert_date) as insert_date
    
      , sum(attributeddetailpageviewsclicks14d) as pageviews
      , sum(attributedconversions14d) as conversions_platform
      , sum(attributedsales14d) as revenue_platform
      , sum(attributedconversions14d) as conversions_ga
      , sum(attributedsales14d) as revenue_ga
      , sum(attributedordersnewtobrand14d) as conversions_ntb
      , sum(attributedsalesnewtobrand14d) as revenue_ntb
      , sum(impressions) as impressions
      , sum(clicks) as clicks
      , sum(cost) as spend

    from source  
    {{ dbt_utils.group_by(10) }}
  )

  ,filtered as (
    select   
      date
      ,campaign
      ,product
      ,product_category
      ,case
          when lower(tactic) like '%nonbrand%' then 'Nonbrand'
          when lower(tactic) like '%brand%' then 'Brand'
          else 'Unknown'
       end as campaign_branded
      ,tactic
      ,engine
      ,channel
      ,connection
      ,_dw_connection
      ,insert_date

      ,pageviews
      ,conversions_platform
      ,conversions_ga
      ,revenue_platform
      ,revenue_ga
      ,conversions_ntb
      ,revenue_ntb
      ,impressions
      ,clicks
      ,spend
      ,case
          when (engine = 'AmazonDSP' or engine = 'AmazonSD' or engine = 'AmazonSB') then conversions_platform
       end as conv_ntb
      ,case
          when (engine = 'AmazonDSP' or engine = 'AmazonSD' or engine = 'AmazonSB') then revenue_platform
       end as rev_ntb

    from aggregated
  )

select *

from filtered